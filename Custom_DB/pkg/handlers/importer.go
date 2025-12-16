package importer

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"os/exec"

	"strings"

	"Custom_DB/pkg/schema"
	"Custom_DB/pkg/storage"
)

// ImportCSV reads a CSV file and appends rows to the destination table
func ImportCSV(path string, db *schema.Database, tableName string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open CSV '%s': %w", path, err)
	}
	defer f.Close()

	r := csv.NewReader(bufio.NewReader(f))
	r.TrimLeadingSpace = true

	header, err := r.Read()
	if err != nil {
		return fmt.Errorf("failed to read CSV header: %w", err)
	}
	if len(header) == 0 {
		return fmt.Errorf("CSV header is empty")
	}

	// If destination table doesn't exist, create it using CSV header (all TEXT)
	_, ok := db.GetTable(tableName)
	if !ok {
		cols := make([]schema.Column, 0, len(header))
		for _, h := range header {
			name := strings.TrimSpace(h)
			if name == "" {
				continue
			}
			cols = append(cols, schema.Column{Name: name, Type: schema.Text})
		}
		newTable := schema.Table{Name: tableName, Columns: cols}
		if err := db.AddTable(newTable); err != nil {
			return fmt.Errorf("failed to create table '%s': %w", tableName, err)
		}
	}

	tf, err := storage.NewTableFile(db.GetDBPath(), tableName)
	if err != nil {
		return fmt.Errorf("failed to open table file: %w", err)
	}

	for {
		rec, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("error reading CSV: %w", err)
		}
		if len(rec) == 0 {
			continue
		}

		row := make(map[string]interface{})
		for i, cell := range rec {
			if i >= len(header) {
				break
			}
			col := strings.TrimSpace(header[i])
			row[col] = strings.TrimSpace(cell)
		}

		if err := tf.AppendRow(row); err != nil {
			return fmt.Errorf("failed to append row: %w", err)
		}
	}

	return nil
}

// ImportParquet is a stub; real parquet support requires a dependency.
func ImportParquet(path string, db *schema.Database, tableName string) error {
	// Try to convert parquet to CSV using local CLI tools (parquet-tools or parquet2csv)
	cmds := [][]string{
		{"parquet-tools", "csv", path},
		{"parquet-tools", "cat", path},
		{"parquet2csv", path},
	}

	var out []byte
	var execErr error
	for _, c := range cmds {
		// check availability
		if _, err := exec.LookPath(c[0]); err != nil {
			continue
		}
		cmd := exec.Command(c[0], c[1:]...)
		out, execErr = cmd.Output()
		if execErr == nil {
			break
		}
	}

	if execErr != nil || len(out) == 0 {
		// Try python fallback (pandas + pyarrow)
		pyScript := `import sys
import pandas as pd
fn = sys.argv[1]
df = pd.read_parquet(fn)
print(df.to_csv(index=False))
`
		// try local virtualenv python first (./.venv or ./venv), then system python3
		pythonCandidates := []string{"./.venv/bin/python3", "./venv/bin/python3", "python3"}
		var pyExec string
		for _, p := range pythonCandidates {
			if path, err := exec.LookPath(p); err == nil {
				pyExec = path
				break
			}
		}
		if pyExec != "" {
			// write CSV output to a temp file then call ImportCSV to reuse logic
			tmpf, err := os.CreateTemp("", "parquet_conv_*.csv")
			if err != nil {
				return fmt.Errorf("failed to create temp file for parquet conversion: %w", err)
			}
			defer os.Remove(tmpf.Name())
			cmd := exec.Command(pyExec, "-c", pyScript, path)
			cmd.Stdout = tmpf
			cmd.Stderr = os.Stderr
			if err := cmd.Run(); err != nil {
				return fmt.Errorf("python parquet->csv conversion failed (using %s): %w", pyExec, err)
			}
			// flush and reopen temp file for reading via ImportCSV
			if err := tmpf.Close(); err != nil {
				return fmt.Errorf("failed to close temp csv file: %w", err)
			}
			return ImportCSV(tmpf.Name(), db, tableName)
		}
		return fmt.Errorf("parquet import requires an external converter (e.g. 'parquet-tools' or 'parquet2csv') or python with pandas installed; error: %v", execErr)
	}

	// Parse CSV output from converter
	r := csv.NewReader(bytes.NewReader(out))
	r.TrimLeadingSpace = true

	header, err := r.Read()
	if err != nil {
		return fmt.Errorf("failed to read CSV header from parquet converter output: %w", err)
	}

	// Create table if missing
	_, ok := db.GetTable(tableName)
	if !ok {
		cols := make([]schema.Column, 0, len(header))
		for _, h := range header {
			name := strings.TrimSpace(h)
			if name == "" {
				continue
			}
			cols = append(cols, schema.Column{Name: name, Type: schema.Text})
		}
		newTable := schema.Table{Name: tableName, Columns: cols}
		if err := db.AddTable(newTable); err != nil {
			return fmt.Errorf("failed to create table '%s': %w", tableName, err)
		}
	}

	tf, err := storage.NewTableFile(db.GetDBPath(), tableName)
	if err != nil {
		return fmt.Errorf("failed to open table file: %w", err)
	}

	for {
		rec, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("error reading CSV from parquet converter output: %w", err)
		}
		if len(rec) == 0 {
			continue
		}
		row := make(map[string]interface{})
		for i, cell := range rec {
			if i >= len(header) {
				break
			}
			col := strings.TrimSpace(header[i])
			row[col] = strings.TrimSpace(cell)
		}
		if err := tf.AppendRow(row); err != nil {
			return fmt.Errorf("failed to append row: %w", err)
		}
	}
	return nil
}
