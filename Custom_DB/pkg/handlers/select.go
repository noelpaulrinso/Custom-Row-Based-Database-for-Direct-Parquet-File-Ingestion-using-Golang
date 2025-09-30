package handlers

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"Custom_DB/pkg/expr"
	"Custom_DB/pkg/parser"
	"Custom_DB/pkg/schema"
	"Custom_DB/pkg/storage"
)

// HandleSelect executes a SELECT command represented by parser.Command against db and
// returns a printable result string.
func HandleSelect(cmd parser.Command, db *schema.Database) (string, error) {
	tokens := cmd.Tokens
	if len(tokens) == 0 {
		return "", fmt.Errorf("empty command")
	}

	// find clause indices
	fromIdx := parser.IndexOfKeyword(cmd, "FROM")
	if fromIdx == -1 || fromIdx+1 >= len(tokens) {
		return "", fmt.Errorf("missing FROM or table name")
	}

	// find WHERE, GROUP, HAVING, ORDER, LIMIT/OFFSET
	whereIdx := parser.IndexOfKeyword(cmd, "WHERE")
	groupIdx := parser.IndexOfKeyword(cmd, "GROUP")
	// ensure GROUP is followed by BY
	if groupIdx != -1 && groupIdx+1 < len(tokens) && strings.ToUpper(tokens[groupIdx+1]) != "BY" {
		groupIdx = -1
	}
	orderIdx := parser.IndexOfKeyword(cmd, "ORDER")
	if orderIdx != -1 && orderIdx+1 < len(tokens) && strings.ToUpper(tokens[orderIdx+1]) != "BY" {
		orderIdx = -1
	}
	havingIdx := parser.IndexOfKeyword(cmd, "HAVING")
	limitIdx := parser.IndexOfKeyword(cmd, "LIMIT")
	offsetIdx := parser.IndexOfKeyword(cmd, "OFFSET")

	// select expression tokens: tokens[1:fromIdx]
	selTokens := tokens[1:fromIdx]
	selExpr := strings.Join(selTokens, " ")
	selExpr = strings.TrimSpace(selExpr)

	// table name
	tableName := tokens[fromIdx+1]

	table, exists := db.GetTable(tableName)
	if !exists {
		return "", fmt.Errorf("table '%s' does not exist", tableName)
	}

	distinct := false
	upSel := strings.ToUpper(selExpr)
	if strings.HasPrefix(upSel, "DISTINCT ") {
		distinct = true
		selExpr = strings.TrimSpace(selExpr[len("DISTINCT "):])
	}

	// parse projection columns into specs (support aggregates)
	type projSpec struct {
		raw     string // original text
		isAgg   bool
		aggFunc string // COUNT, SUM, AVG, MIN, MAX
		aggCol  string // column for agg (or "*")
		outName string // output column name to produce
		col     string // simple column name when not aggregate
		alias   string // alias if provided
	}
	projSpecs := []projSpec{}
	if selExpr == "*" {
		for _, c := range table.Columns {
			projSpecs = append(projSpecs, projSpec{raw: c.Name, isAgg: false, col: c.Name, outName: c.Name})
		}
	} else {
		for _, part := range strings.Split(selExpr, ",") {
			p := strings.TrimSpace(part)
			if p == "" {
				continue
			}
			// handle alias via AS
			asParts := strings.SplitN(p, " AS ", 2)
			if len(asParts) == 1 {
				asParts = strings.SplitN(p, " as ", 2)
			}
			exprText := strings.TrimSpace(asParts[0])
			alias := ""
			if len(asParts) == 2 {
				alias = strings.TrimSpace(asParts[1])
				alias = strings.Trim(alias, "`\"")
			}
			// normalize spacing: remove space before/after parentheses to handle tokenizer spacing
			exprText = strings.ReplaceAll(exprText, " (", "(")
			exprText = strings.ReplaceAll(exprText, "( ", "(")
			exprText = strings.ReplaceAll(exprText, " )", ")")
			up := strings.ToUpper(exprText)
			spec := projSpec{raw: exprText, alias: alias}
			// detect aggregate forms COUNT(*), COUNT(col), SUM(col), AVG(col), MIN(col), MAX(col)
			if strings.HasPrefix(up, "COUNT(") {
				spec.isAgg = true
				spec.aggFunc = "COUNT"
				inside := strings.TrimSpace(exprText[6 : len(exprText)-1])
				spec.aggCol = strings.TrimSpace(inside)
				if spec.aggCol == "*" {
					spec.outName = "count"
				} else {
					spec.outName = "count_" + strings.Trim(spec.aggCol, "`\"")
				}
			} else if strings.HasPrefix(up, "SUM(") || strings.HasPrefix(up, "AVG(") || strings.HasPrefix(up, "MIN(") || strings.HasPrefix(up, "MAX(") {
				// generic parse: FUNC(col)
				idx := strings.Index(up, "(")
				fn := strings.ToUpper(up[:idx])
				inside := strings.TrimSpace(exprText[idx+1 : len(exprText)-1])
				spec.isAgg = true
				spec.aggFunc = fn
				spec.aggCol = strings.Trim(inside, "`\"")
				spec.outName = strings.ToLower(fn) + "_" + spec.aggCol
			} else {
				// simple column
				fields := strings.Fields(exprText)
				col := strings.Trim(fields[0], "`\"")
				spec.isAgg = false
				spec.col = col
				spec.outName = col
			}
			if spec.alias != "" {
				spec.outName = spec.alias
			}
			projSpecs = append(projSpecs, spec)
		}
	}

	// determine bounds for clauses (used for WHERE parsing)
	endWhere := len(tokens)
	candidates := []int{groupIdx, havingIdx, orderIdx, limitIdx, offsetIdx}
	for _, v := range candidates {
		if v != -1 && v < endWhere {
			endWhere = v
		}
	}

	// WHERE: parse expression into AST and evaluate per-row
	var whereExpr expr.Expr
	if whereIdx != -1 {
		raw := strings.Join(tokens[whereIdx+1:endWhere], " ")
		e, perr := expr.ParseExpression(raw)
		if perr != nil {
			return "", fmt.Errorf("invalid WHERE expression: %w", perr)
		}
		// validate referenced columns exist in table schema
		cols := expr.CollectColumns(e)
		for _, c := range cols {
			if _, ok := getColumnDefinition(table.Columns, c); !ok {
				return "", fmt.Errorf("WHERE references unknown column '%s'", c)
			}
		}
		whereExpr = e
	}

	// GROUP BY handling
	var grouping bool
	var groupCol string
	if groupIdx != -1 {
		// group by column expected at groupIdx+2
		if groupIdx+2 < len(tokens) {
			groupCol = strings.Trim(tokens[groupIdx+2], "`\"")
			grouping = true
		}
	}
	// if there are aggregate projections but no explicit GROUP BY, treat as global aggregation (single group)
	hasAggProj := false
	for _, ps := range projSpecs {
		if ps.isAgg {
			hasAggProj = true
			break
		}
	}
	if !grouping && hasAggProj {
		grouping = true
		groupCol = "" // empty key for global aggregation
	}

	// if user explicitly provided GROUP BY but no aggregate projections, be lenient and add COUNT(*) automatically
	if grouping && !hasAggProj {
		// add COUNT(*) default
		projSpecs = append(projSpecs, projSpec{raw: "COUNT(*)", isAgg: true, aggFunc: "COUNT", aggCol: "*", outName: "count"})
		hasAggProj = true
	}

	// ORDER BY handling
	var orderCol string
	orderAsc := true
	if orderIdx != -1 {
		if orderIdx+2 < len(tokens) {
			orderCol = strings.Trim(tokens[orderIdx+2], "`\"")
			if orderIdx+3 < len(tokens) && strings.ToUpper(tokens[orderIdx+3]) == "DESC" {
				orderAsc = false
			}
		}
	}

	// read rows
	tableFile, err := storage.NewTableFile(db.GetDBPath(), tableName)
	if err != nil {
		return "", err
	}
	rows, err := tableFile.ReadAllRows()
	if err != nil {
		return "", err
	}

	// apply WHERE via AST evaluator
	if whereExpr != nil {
		filtered := make([]storage.Row, 0, len(rows))
		for _, r := range rows {
			ok, err := whereExpr.Eval(r)
			if err != nil {
				return "", fmt.Errorf("error evaluating WHERE: %w", err)
			}
			if ok {
				filtered = append(filtered, r)
			}
		}
		rows = filtered
	}
	// GROUP BY / Aggregation handling
	if grouping {
		// validate group column exists (unless global aggregation with empty groupCol)
		if groupCol != "" {
			if _, ok := getColumnDefinition(table.Columns, groupCol); !ok {
				return "", fmt.Errorf("GROUP BY references unknown column '%s'", groupCol)
			}
		}

		// prepare aggregation maps keyed by group key string
		counts := make(map[string]int)
		sums := make(map[string]map[string]float64) // group -> outName -> sum
		mins := make(map[string]map[string]float64)
		maxs := make(map[string]map[string]float64)
		cntsForAvg := make(map[string]map[string]int)
		sample := make(map[string]map[string]interface{}) // group -> sample values (including groupCol)

		for _, r := range rows {
			var key string
			if groupCol == "" {
				key = "__global__"
			} else {
				key = fmt.Sprintf("%v", r[groupCol])
			}
			counts[key]++
			if _, ok := sample[key]; !ok {
				sample[key] = make(map[string]interface{})
				if groupCol != "" {
					sample[key][groupCol] = r[groupCol]
				}
			}
			// initialize maps
			if _, ok := sums[key]; !ok {
				sums[key] = map[string]float64{}
				mins[key] = map[string]float64{}
				maxs[key] = map[string]float64{}
				cntsForAvg[key] = map[string]int{}
			}
			// update aggregates per projSpec
			for _, ps := range projSpecs {
				if !ps.isAgg {
					continue
				}
				switch ps.aggFunc {
				case "COUNT":
					// count handled by counts[key] or count of specific column if provided
					if ps.aggCol != "*" && ps.aggCol != "" {
						if _, ok := r[ps.aggCol]; ok {
							// treat non-null as count
							sums[key][ps.outName] = sums[key][ps.outName] + 1
						}
					} else {
						// COUNT(*) -> counts[key] later
						// we'll set outName from counts map
					}
				case "SUM", "AVG":
					if v, ok := r[ps.aggCol]; ok {
						if f, okf := toFloat(v); okf {
							sums[key][ps.outName] += f
							cntsForAvg[key][ps.outName]++
							if _, has := mins[key][ps.outName]; !has || f < mins[key][ps.outName] {
								mins[key][ps.outName] = f
							}
							if _, has := maxs[key][ps.outName]; !has || f > maxs[key][ps.outName] {
								maxs[key][ps.outName] = f
							}
						}
					}
				case "MIN":
					if v, ok := r[ps.aggCol]; ok {
						if f, okf := toFloat(v); okf {
							if _, has := mins[key][ps.outName]; !has || f < mins[key][ps.outName] {
								mins[key][ps.outName] = f
							}
						}
					}
				case "MAX":
					if v, ok := r[ps.aggCol]; ok {
						if f, okf := toFloat(v); okf {
							if _, has := maxs[key][ps.outName]; !has || f > maxs[key][ps.outName] {
								maxs[key][ps.outName] = f
							}
						}
					}
				}
			}
		}

		// build aggregated rows
		aggRows := make([]storage.Row, 0, len(counts))
		for k, cnt := range counts {
			nr := make(storage.Row)
			if groupCol != "" {
				nr[groupCol] = sample[k][groupCol]
			}
			// set COUNT(*) if requested
			for _, ps := range projSpecs {
				if ps.isAgg && ps.aggFunc == "COUNT" {
					if ps.aggCol == "*" || ps.aggCol == "" {
						nr[ps.outName] = cnt
					} else {
						// we used sums map to count non-* counts
						if v, ok := sums[k][ps.outName]; ok {
							nr[ps.outName] = int(v)
						} else {
							nr[ps.outName] = 0
						}
					}
				}
				if ps.isAgg && (ps.aggFunc == "SUM" || ps.aggFunc == "AVG") {
					s := sums[k][ps.outName]
					if ps.aggFunc == "SUM" {
						nr[ps.outName] = s
					} else {
						cntv := cntsForAvg[k][ps.outName]
						if cntv == 0 {
							nr[ps.outName] = 0
						} else {
							nr[ps.outName] = s / float64(cntv)
						}
					}
				}
				if ps.isAgg && ps.aggFunc == "MIN" {
					if v, ok := mins[k][ps.outName]; ok {
						nr[ps.outName] = v
					} else {
						nr[ps.outName] = nil
					}
				}
				if ps.isAgg && ps.aggFunc == "MAX" {
					if v, ok := maxs[k][ps.outName]; ok {
						nr[ps.outName] = v
					} else {
						nr[ps.outName] = nil
					}
				}
			}
			aggRows = append(aggRows, nr)
		}

		// HAVING: if present, evaluate against aggregated rows. We allow HAVING to reference aggregates by the same function syntax;
		// we rewrite occurrences of aggregate function calls to the generated outName before parsing.
		if havingIdx != -1 {
			// determine having bounds
			endHaving := len(tokens)
			hCandidates := []int{orderIdx, limitIdx, offsetIdx}
			for _, v := range hCandidates {
				if v != -1 && v < endHaving {
					endHaving = v
				}
			}
			rawHaving := strings.Join(tokens[havingIdx+1:endHaving], " ")
			// rewrite aggregate function references to produced outNames
			rewrite := func(s string) string {
				out := s
				for _, ps := range projSpecs {
					if !ps.isAgg {
						continue
					}
					// replace function call text with outName (case-insensitive)
					old := ps.raw
					// if COUNT(*) normalize old to COUNT(*)
					if strings.ToUpper(old) == "COUNT(*)" {
						old = "COUNT(*)"
					}
					out = replaceIgnoreCase(out, old, ps.outName)
					// also support COUNT(col) variations
					// attempt to replace function style occurrences by constructing pattern
					patt := strings.ToUpper(ps.aggFunc) + "(" + strings.ToUpper(ps.aggCol) + ")"
					out = replaceIgnoreCase(out, patt, ps.outName)
				}
				return out
			}
			rewritten := rewrite(rawHaving)
			he, herr := expr.ParseExpression(rewritten)
			if herr != nil {
				return "", fmt.Errorf("invalid HAVING expression: %w", herr)
			}
			// validate referenced columns exist in aggregated rows (use first agg row as sample)
			sampleRow := storage.Row{}
			if len(aggRows) > 0 {
				sampleRow = aggRows[0]
			}
			for _, c := range expr.CollectColumns(he) {
				if _, ok := sampleRow[c]; !ok {
					return "", fmt.Errorf("HAVING references unknown aggregate/column '%s'", c)
				}
			}
			// filter
			filteredAgg := make([]storage.Row, 0, len(aggRows))
			for _, ar := range aggRows {
				ok, err := he.Eval(ar)
				if err != nil {
					return "", fmt.Errorf("error evaluating HAVING: %w", err)
				}
				if ok {
					filteredAgg = append(filteredAgg, ar)
				}
			}
			aggRows = filteredAgg
		}

		// ORDER on aggregated rows
		if orderCol != "" {
			sort.Slice(aggRows, func(i, j int) bool {
				si := fmt.Sprintf("%v", aggRows[i][orderCol])
				sj := fmt.Sprintf("%v", aggRows[j][orderCol])
				fi, erri := strconv.ParseFloat(si, 64)
				fj, errj := strconv.ParseFloat(sj, 64)
				if erri == nil && errj == nil {
					if orderAsc {
						return fi < fj
					}
					return fi > fj
				}
				if orderAsc {
					return strings.Compare(si, sj) < 0
				}
				return strings.Compare(si, sj) > 0
			})
		}

		// LIMIT/OFFSET on aggregated rows
		start := 0
		if offsetIdx != -1 && offsetIdx+1 < len(tokens) {
			if v, err := strconv.Atoi(tokens[offsetIdx+1]); err == nil {
				start = v
			}
		}
		end := len(aggRows)
		if limitIdx != -1 && limitIdx+1 < len(tokens) {
			if v, err := strconv.Atoi(tokens[limitIdx+1]); err == nil {
				end = start + v
				if end > len(aggRows) {
					end = len(aggRows)
				}
			}
		}
		if start < 0 {
			start = 0
		}
		if start > len(aggRows) {
			start = len(aggRows)
		}
		aggRows = aggRows[start:end]

		// build output header based on projection specs
		headerCols := []string{}
		for _, ps := range projSpecs {
			headerCols = append(headerCols, ps.outName)
		}
		sb := &strings.Builder{}
		for _, c := range headerCols {
			sb.WriteString(fmt.Sprintf("%-20s", c))
		}
		sb.WriteString("\n")
		sb.WriteString(strings.Repeat("-", 20*len(headerCols)))
		sb.WriteString("\n")
		for _, r := range aggRows {
			for _, ps := range projSpecs {
				if v, ok := r[ps.outName]; ok {
					sb.WriteString(fmt.Sprintf("%-20v", v))
				} else if ps.isAgg {
					sb.WriteString(fmt.Sprintf("%-20v", "NULL"))
				} else {
					// non-agg columns in grouping must be groupCol only
					if ps.col == groupCol {
						if v, ok := r[groupCol]; ok {
							sb.WriteString(fmt.Sprintf("%-20v", v))
						} else {
							sb.WriteString(fmt.Sprintf("%-20s", "NULL"))
						}
					} else {
						if !strings.EqualFold(ps.col, groupCol) {
							return "", fmt.Errorf("cannot select non-aggregated column '%s' without grouping", ps.col)
						}
						// else allowed (case-insensitive match)
					}
				}
			}
			sb.WriteString("\n")
		}
		return sb.String(), nil
	}

	// ORDER for normal rows
	if orderCol != "" && len(rows) > 1 {
		sort.Slice(rows, func(i, j int) bool {
			si := fmt.Sprintf("%v", rows[i][orderCol])
			sj := fmt.Sprintf("%v", rows[j][orderCol])
			fi, erri := strconv.ParseFloat(si, 64)
			fj, errj := strconv.ParseFloat(sj, 64)
			if erri == nil && errj == nil {
				if orderAsc {
					return fi < fj
				}
				return fi > fj
			}
			if orderAsc {
				return strings.Compare(si, sj) < 0
			}
			return strings.Compare(si, sj) > 0
		})
	}

	// DISTINCT dedupe
	if distinct {
		seen := map[string]struct{}{}
		uniq := make([]storage.Row, 0, len(rows))
		for _, r := range rows {
			parts := make([]string, 0, len(projSpecs))
			for _, ps := range projSpecs {
				var val interface{}
				if ps.isAgg {
					if v, ok := r[ps.outName]; ok {
						val = v
					}
				} else {
					if v, ok := r[ps.col]; ok {
						val = v
					}
				}
				if val != nil {
					parts = append(parts, fmt.Sprintf("%v", val))
				} else {
					parts = append(parts, "NULL")
				}
			}
			key := strings.Join(parts, "|")
			if _, ok := seen[key]; !ok {
				seen[key] = struct{}{}
				uniq = append(uniq, r)
			}
		}
		rows = uniq
	}

	// projection output (non-aggregated rows)
	headerCols := []string{}
	for _, ps := range projSpecs {
		headerCols = append(headerCols, ps.outName)
	}
	sb := &strings.Builder{}
	for _, c := range headerCols {
		sb.WriteString(fmt.Sprintf("%-20s", c))
	}
	sb.WriteString("\n")
	sb.WriteString(strings.Repeat("-", 20*len(headerCols)))
	sb.WriteString("\n")
	for _, r := range rows {
		for _, ps := range projSpecs {
			if ps.isAgg {
				if v, ok := r[ps.outName]; ok {
					sb.WriteString(fmt.Sprintf("%-20v", v))
				} else {
					sb.WriteString(fmt.Sprintf("%-20s", "NULL"))
				}
			} else {
				if v, ok := r[ps.col]; ok {
					sb.WriteString(fmt.Sprintf("%-20v", v))
				} else {
					sb.WriteString(fmt.Sprintf("%-20s", "NULL"))
				}
			}
		}
		sb.WriteString("\n")
	}
	return sb.String(), nil
}

// helper functions copied/adapted from main.go but kept local to handlers package
func getColumnDefinition(columns []schema.Column, colName string) (schema.Column, bool) {
	for _, col := range columns {
		if strings.EqualFold(col.Name, colName) {
			return col, true
		}
	}
	return schema.Column{}, false
}

func coerceValue(valStr string, targetType schema.DataType) (interface{}, error) {
	trimmedVal := strings.TrimSpace(valStr)

	if targetType == schema.Text && len(trimmedVal) > 1 && trimmedVal[0] == '\'' && trimmedVal[len(trimmedVal)-1] == '\'' {
		trimmedVal = trimmedVal[1 : len(trimmedVal)-1]
	}

	switch targetType {
	case schema.Integer:
		return strconv.Atoi(trimmedVal)
	case schema.Decimal:
		return strconv.ParseFloat(trimmedVal, 64)
	case schema.Boolean:
		return strconv.ParseBool(trimmedVal)
	case schema.Text:
		return trimmedVal, nil
	default:
		return nil, fmt.Errorf("unsupported data type for coercion: %s", targetType)
	}
}

// helper: parse numeric-like interface to float64
func toFloat(v interface{}) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case float32:
		return float64(t), true
	case int:
		return float64(t), true
	case int64:
		return float64(t), true
	case string:
		if f, err := strconv.ParseFloat(t, 64); err == nil {
			return f, true
		}
	}
	return 0, false
}

// replaceIgnoreCase replaces old (case-insensitive) occurrences in s with repl
func replaceIgnoreCase(s, old, repl string) string {
	if old == "" {
		return s
	}
	lower := strings.ToLower(s)
	target := strings.ToLower(old)
	var sb strings.Builder
	i := 0
	for {
		idx := strings.Index(lower[i:], target)
		if idx == -1 {
			sb.WriteString(s[i:])
			break
		}
		sb.WriteString(s[i : i+idx])
		sb.WriteString(repl)
		i += idx + len(target)
	}
	return sb.String()
}
