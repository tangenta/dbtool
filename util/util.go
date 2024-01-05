package util

import (
	"database/sql"
	"fmt"
)

func ReadAll(rows *sql.Rows) [][]string {
	defer func() {
		rows.Close()
	}()

	data := make([][]string, 0)
	columns, err := rows.Columns()
	if err != nil {
		panic(err)
	}
	for rows.Next() {
		row := make([]interface{}, len(columns))
		scan := make([]interface{}, len(columns))
		for i := range row {
			scan[i] = &row[i]
		}
		err := rows.Scan(scan...)
		if err != nil {
			panic(err)
		}

		rowstr := make([]string, len(columns))
		for i := range rowstr {
			switch r := row[i].(type) {
			case nil:
				rowstr[i] = "NULL"
			case []byte:
				rowstr[i] = string(r)
			default:
				rowstr[i] = fmt.Sprintf("%v", row[i])
			}
		}
		data = append(data, rowstr)
	}
	err = rows.Err()
	if err != nil {
		panic(err)
	}
	return data
}
