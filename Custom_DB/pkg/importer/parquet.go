package importer

import (
	"Custom_DB/pkg/schema"
	"Custom_DB/pkg/storage"
	"fmt"
	"log"
	"reflect"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

func ImportParquet(filePath string, db *schema.Database) error {
	fr, err := local.NewLocalFileReader(filePath)
	if err != nil {
		return fmt.Errorf("failed to open parquet file: %w", err)
	}
	defer fr.Close()

	pr, err := reader.NewParquetReader(fr, nil, 1)
	if err != nil {
		return fmt.Errorf("failed to create parquet reader: %w", err)
	}
	defer pr.ReadStop()

	numRows := int(pr.GetNumRows())
	tableName := getTableNameFromPath(filePath)

	// Assuming the schema can be inferred from the Parquet file.
	// This is a simplified approach. A real implementation might need a more robust schema detection
	// or a way for the user to define it.
	var columns []schema.Column
	for _, info := range pr.SchemaHandler.Infos {
		if info.ExName == "" {
			continue
		}
		var colType schema.DataType
		switch info.Type {
		case "BOOLEAN":
			colType = schema.Boolean
		case "INT32", "INT64":
			colType = schema.Integer
		case "FLOAT", "DOUBLE":
			colType = schema.Decimal
		case "BYTE_ARRAY", "FIXED_LEN_BYTE_ARRAY":
			colType = schema.Text
		default:
			colType = schema.Text
		}
		columns = append(columns, schema.Column{Name: info.ExName, Type: colType})
	}

	table := schema.Table{
		Name:    tableName,
		Columns: columns,
	}

	if err := db.AddTable(table); err != nil {
		log.Printf("Table %s might already exist, proceeding with data insertion. Error: %v", tableName, err)
	}

	tableFile, err := storage.NewTableFile(db.GetDBPath(), tableName)
	if err != nil {
		return fmt.Errorf("failed to create table file: %w", err)
	}

	for i := 0; i < numRows; i++ {
		rows, err := pr.ReadByNumber(1)
		if err != nil {
			return fmt.Errorf("failed to read row %d: %w", i, err)
		}
		if len(rows) == 0 {
			continue
		}

		row := rows[0]
		var storageRow storage.Row
		switch r := row.(type) {
		case map[string]interface{}:
			storageRow = make(storage.Row)
			for k, v := range r {
				storageRow[k] = v
			}
		default:
			// Use reflection to convert struct to map[string]interface{}
			val := reflect.ValueOf(row)
			if val.Kind() == reflect.Struct {
				storageRow = make(storage.Row)
				typ := val.Type()
				for j := 0; j < val.NumField(); j++ {
					field := typ.Field(j)
					fieldName := field.Name
					fieldValue := val.Field(j).Interface()
					storageRow[fieldName] = fieldValue
				}
			} else {
				log.Printf("Unknown row type at %d: %T", i, row)
				continue
			}
		}

		if err := tableFile.AppendRow(storageRow); err != nil {
			log.Printf("failed to append row %d: %v", i, err)
		}
	}

	fmt.Printf("Successfully imported %d rows into table '%s' from Parquet file.\n", numRows, tableName)
	return nil
}
