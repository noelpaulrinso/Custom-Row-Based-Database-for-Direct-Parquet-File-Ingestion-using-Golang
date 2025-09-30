package expr

import (
	"fmt"
	"strconv"
	"strings"

	"Custom_DB/pkg/storage"
)

// Expr is a boolean expression that can be evaluated against a row.
type Expr interface {
	Eval(row storage.Row) (bool, error)
}

// operand abstraction (column ref or literal)
type operand struct {
	isColumn bool
	col      string
	lit      interface{}
}

func (o *operand) Value(row storage.Row) (interface{}, bool) {
	if o.isColumn {
		v, ok := row[o.col]
		return v, ok
	}
	return o.lit, true
}

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

// ----- AST nodes -----

type binaryOp struct {
	op    string
	left  Expr
	right Expr
}

func (b *binaryOp) Eval(row storage.Row) (bool, error) {
	l, err := b.left.Eval(row)
	if err != nil {
		return false, err
	}
	if strings.EqualFold(b.op, "AND") {
		if !l {
			return false, nil
		}
		r, err := b.right.Eval(row)
		return r, err
	}
	if strings.EqualFold(b.op, "OR") {
		if l {
			return true, nil
		}
		r, err := b.right.Eval(row)
		return r, err
	}
	return false, fmt.Errorf("unsupported binary op: %s", b.op)
}

type notOp struct{ child Expr }

func (n *notOp) Eval(row storage.Row) (bool, error) {
	v, err := n.child.Eval(row)
	if err != nil {
		return false, err
	}
	return !v, nil
}

// comparison node
type compOp struct {
	op    string
	left  operand
	right operand
}

func (c *compOp) Eval(row storage.Row) (bool, error) {
	lv, lok := c.left.Value(row)
	rv, rok := c.right.Value(row)
	// if either side is a column reference that doesn't exist, error
	if c.left.isColumn && !lok {
		return false, fmt.Errorf("column '%s' not found", c.left.col)
	}
	if c.right.isColumn && !rok {
		return false, fmt.Errorf("column '%s' not found", c.right.col)
	}
	// if either operand is a sub-expression (Expr), evaluate it
	if se, ok := lv.(Expr); ok {
		b, err := se.Eval(row)
		if err != nil {
			return false, err
		}
		lv = b
	}
	if se, ok := rv.(Expr); ok {
		b, err := se.Eval(row)
		if err != nil {
			return false, err
		}
		rv = b
	}
	lf, lnum := toFloat(lv)
	rf, rnum := toFloat(rv)
	switch c.op {
	case "=":
		if lnum && rnum {
			return lf == rf, nil
		}
		return fmt.Sprintf("%v", lv) == fmt.Sprintf("%v", rv), nil
	case "!=":
		if lnum && rnum {
			return lf != rf, nil
		}
		return fmt.Sprintf("%v", lv) != fmt.Sprintf("%v", rv), nil
	case "<":
		if lnum && rnum {
			return lf < rf, nil
		}
		return fmt.Sprintf("%v", lv) < fmt.Sprintf("%v", rv), nil
	case "<=":
		if lnum && rnum {
			return lf <= rf, nil
		}
		return fmt.Sprintf("%v", lv) <= fmt.Sprintf("%v", rv), nil
	case ">":
		if lnum && rnum {
			return lf > rf, nil
		}
		return fmt.Sprintf("%v", lv) > fmt.Sprintf("%v", rv), nil
	case ">=":
		if lnum && rnum {
			return lf >= rf, nil
		}
		return fmt.Sprintf("%v", lv) >= fmt.Sprintf("%v", rv), nil
	}
	return false, fmt.Errorf("unsupported comp op: %s", c.op)
}

// IN node
type inOp struct {
	left operand
	list []operand
}

func (i *inOp) Eval(row storage.Row) (bool, error) {
	lv, lok := i.left.Value(row)
	if i.left.isColumn && !lok {
		return false, fmt.Errorf("column '%s' not found", i.left.col)
	}
	if se, ok := lv.(Expr); ok {
		b, err := se.Eval(row)
		if err != nil {
			return false, err
		}
		lv = b
	}
	for _, it := range i.list {
		v, rok := it.Value(row)
		if it.isColumn && !rok {
			return false, fmt.Errorf("column '%s' not found", it.col)
		}
		if se, ok := v.(Expr); ok {
			b, err := se.Eval(row)
			if err != nil {
				return false, err
			}
			v = b
		}
		if fmt.Sprintf("%v", lv) == fmt.Sprintf("%v", v) {
			return true, nil
		}
	}
	return false, nil
}

// BETWEEN node
type betweenOp struct {
	left operand
	lo   operand
	hi   operand
}

func (b *betweenOp) Eval(row storage.Row) (bool, error) {
	lv, lok := b.left.Value(row)
	if b.left.isColumn && !lok {
		return false, fmt.Errorf("column '%s' not found", b.left.col)
	}
	// evaluate subexprs
	if se, ok := lv.(Expr); ok {
		bb, err := se.Eval(row)
		if err != nil {
			return false, err
		}
		lv = bb
	}
	lofV, lok2 := b.lo.Value(row)
	if b.lo.isColumn && !lok2 {
		return false, fmt.Errorf("column '%s' not found", b.lo.col)
	}
	if se, ok := lofV.(Expr); ok {
		bb, err := se.Eval(row)
		if err != nil {
			return false, err
		}
		lofV = bb
	}
	hifV, hik2 := b.hi.Value(row)
	if b.hi.isColumn && !hik2 {
		return false, fmt.Errorf("column '%s' not found", b.hi.col)
	}
	if se, ok := hifV.(Expr); ok {
		bb, err := se.Eval(row)
		if err != nil {
			return false, err
		}
		hifV = bb
	}
	lof, lok := toFloat(lofV)
	hif, hik := toFloat(hifV)
	lf, lnum := toFloat(lv)
	if lnum && lok && hik {
		return lf >= lof && lf <= hif, nil
	}
	ls := fmt.Sprintf("%v", lv)
	loS := fmt.Sprintf("%v", lofV)
	hiS := fmt.Sprintf("%v", hifV)
	return ls >= loS && ls <= hiS, nil
}

// LIKE node
type likeOp struct {
	left    operand
	pattern string
}

func (l *likeOp) Eval(row storage.Row) (bool, error) {
	lv, lok := l.left.Value(row)
	if l.left.isColumn && !lok {
		return false, fmt.Errorf("column '%s' not found", l.left.col)
	}
	if se, ok := lv.(Expr); ok {
		b, err := se.Eval(row)
		if err != nil {
			return false, err
		}
		lv = b
	}
	s := fmt.Sprintf("%v", lv)
	p := l.pattern
	if strings.HasPrefix(p, "%") && strings.HasSuffix(p, "%") {
		return strings.Contains(s, strings.Trim(p, "%")), nil
	} else if strings.HasPrefix(p, "%") {
		return strings.HasSuffix(s, strings.TrimLeft(p, "%")), nil
	} else if strings.HasSuffix(p, "%") {
		return strings.HasPrefix(s, strings.TrimRight(p, "%")), nil
	}
	return s == p, nil
}

// ----- Parser (simple recursive descent) -----

type parser struct {
	toks []string
	pos  int
}

func ParseExpression(raw string) (Expr, error) {
	toks := tokenizeExpr(raw)
	p := &parser{toks: toks, pos: 0}
	return p.parseOr()
}

// CollectColumns returns a list of column names referenced by the expression.
func CollectColumns(e Expr) []string {
	cols := []string{}
	var walk func(Expr)
	walk = func(x Expr) {
		switch v := x.(type) {
		case *binaryOp:
			walk(v.left)
			walk(v.right)
		case *notOp:
			walk(v.child)
		case *compOp:
			if v.left.isColumn {
				cols = append(cols, v.left.col)
			}
			if v.right.isColumn {
				cols = append(cols, v.right.col)
			}
		case *inOp:
			if v.left.isColumn {
				cols = append(cols, v.left.col)
			}
			for _, it := range v.list {
				if it.isColumn {
					cols = append(cols, it.col)
				}
			}
		case *betweenOp:
			if v.left.isColumn {
				cols = append(cols, v.left.col)
			}
			if v.lo.isColumn {
				cols = append(cols, v.lo.col)
			}
			if v.hi.isColumn {
				cols = append(cols, v.hi.col)
			}
		case *likeOp:
			if v.left.isColumn {
				cols = append(cols, v.left.col)
			}
		}
	}
	if e != nil {
		walk(e)
	}
	// dedupe
	seen := map[string]struct{}{}
	out := []string{}
	for _, c := range cols {
		if _, ok := seen[c]; !ok {
			seen[c] = struct{}{}
			out = append(out, c)
		}
	}
	return out
}

func tokenizeExpr(s string) []string {
	var out []string
	var cur strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '\'' {
			cur.WriteByte(c)
			i++
			for i < len(s) && s[i] != '\'' {
				cur.WriteByte(s[i])
				i++
			}
			if i < len(s) {
				cur.WriteByte('\'')
			}
			continue
		}
		switch c {
		case ' ', '\t', '\n', '\r':
			if cur.Len() > 0 {
				out = append(out, cur.String())
				cur.Reset()
			}
		case '(', ')', ',', '=':
			if cur.Len() > 0 {
				out = append(out, cur.String())
				cur.Reset()
			}
			out = append(out, string(c))
		case '!', '<', '>':
			if cur.Len() > 0 {
				out = append(out, cur.String())
				cur.Reset()
			}
			if i+1 < len(s) && s[i+1] == '=' {
				out = append(out, s[i:i+2])
				i++
			} else {
				out = append(out, string(c))
			}
		default:
			cur.WriteByte(c)
		}
	}
	if cur.Len() > 0 {
		out = append(out, cur.String())
	}
	for i := range out {
		out[i] = strings.TrimSpace(out[i])
	}
	return out
}

func (p *parser) cur() string {
	if p.pos >= len(p.toks) {
		return ""
	}
	return p.toks[p.pos]
}
func (p *parser) eat() string { t := p.cur(); p.pos++; return t }

func (p *parser) parseOr() (Expr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for strings.EqualFold(p.cur(), "OR") {
		p.eat()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &binaryOp{op: "OR", left: left, right: right}
	}
	return left, nil
}

func (p *parser) parseAnd() (Expr, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	for strings.EqualFold(p.cur(), "AND") {
		p.eat()
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		left = &binaryOp{op: "AND", left: left, right: right}
	}
	return left, nil
}

func (p *parser) parseNot() (Expr, error) {
	if strings.EqualFold(p.cur(), "NOT") {
		p.eat()
		child, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return &notOp{child: child}, nil
	}
	return p.parseComparison()
}

func (p *parser) parseComparison() (Expr, error) {
	prim, err := p.parsePrimaryOperand()
	if err != nil {
		return nil, err
	}
	cur := strings.ToUpper(p.cur())
	switch cur {
	case "=", "!=", "<", ">", "<=", ">=":
		op := p.eat()
		right, err := p.parsePrimaryOperand()
		if err != nil {
			return nil, err
		}
		return &compOp{op: op, left: prim, right: right}, nil
	case "BETWEEN":
		p.eat()
		lo, err := p.parsePrimaryOperand()
		if err != nil {
			return nil, err
		}
		if !strings.EqualFold(p.cur(), "AND") {
			return nil, fmt.Errorf("BETWEEN missing AND")
		}
		p.eat()
		hi, err := p.parsePrimaryOperand()
		if err != nil {
			return nil, err
		}
		return &betweenOp{left: prim, lo: lo, hi: hi}, nil
	case "IN":
		p.eat()
		if p.cur() != "(" {
			return nil, fmt.Errorf("IN expects (list)")
		}
		p.eat()
		list := []operand{}
		for {
			opd, err := p.parsePrimaryOperand()
			if err != nil {
				return nil, err
			}
			list = append(list, opd)
			if p.cur() == "," {
				p.eat()
				continue
			}
			break
		}
		if p.cur() != ")" {
			return nil, fmt.Errorf("IN expects )")
		}
		p.eat()
		return &inOp{left: prim, list: list}, nil
	default:
		if strings.EqualFold(p.cur(), "LIKE") {
			p.eat()
			pat := p.eat()
			pat = strings.Trim(pat, "'\"")
			return &likeOp{left: prim, pattern: pat}, nil
		}
		// fallback: treat as boolean comparison to true
		return &compOp{op: "!=", left: prim, right: operand{lit: false}}, nil
	}
}

func (p *parser) parsePrimaryOperand() (operand, error) {
	cur := p.cur()
	if cur == "" {
		return operand{}, fmt.Errorf("unexpected end")
	}
	if cur == "(" {
		p.eat()
		sub, err := p.parseOr()
		if err != nil {
			return operand{}, err
		}
		if p.cur() != ")" {
			return operand{}, fmt.Errorf("missing )")
		}
		p.eat()
		// represent boolean subexpr as literal by storing it as lit (Expr) and handling in Eval via fmt
		return operand{isColumn: false, lit: sub}, nil
	}
	if strings.HasPrefix(cur, "'") && strings.HasSuffix(cur, "'") && len(cur) >= 2 {
		p.eat()
		return operand{isColumn: false, lit: strings.Trim(cur, "'")}, nil
	}
	if _, err := strconv.ParseFloat(cur, 64); err == nil {
		p.eat()
		return operand{isColumn: false, lit: cur}, nil
	}
	// identifier
	p.eat()
	return operand{isColumn: true, col: cur}, nil
}
