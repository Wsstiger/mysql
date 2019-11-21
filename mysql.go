package mysql

import (
	"database/sql"
	//"database/sql/driver"
	//"errors"
	// "fmt"
	"github.com/arnehormann/sqlinternals/mysqlinternals"
	_ "github.com/go-sql-driver/mysql"
	"github.com/shopspring/decimal"

	//"reflect"
	"errors"
	"strconv"
)

func NewConnection(dataSourceName string, maxOpenConn, maxIdleConn int) (*sql.DB, error) {
	conn, err := sql.Open("mysql", dataSourceName)
	if err != nil {
		return nil, err
	}
	conn.SetMaxOpenConns(maxOpenConn)
	conn.SetMaxIdleConns(maxIdleConn)

	if err := conn.Ping(); err != nil {
		defer conn.Close()
		return nil, err
	}

	return conn, err
}

func QueryDB(conn *sql.DB, query string, args ...interface{}) ([]map[string]interface{}, error) {
	rows, err := conn.Query(query, args...)
	if err != nil {
		return []map[string]interface{}{}, err
	}
	defer rows.Close()
	// 返回属性字典
	columns, err := mysqlinternals.Columns(rows)
	if err != nil {
		return []map[string]interface{}{}, err
	}
	// 获取字段类型
	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(columns))
	for i, _ := range values {
		scanArgs[i] = &values[i]
	}
	rows_map := make([]map[string]interface{}, 0, 10)
	for rows.Next() {
		rows.Scan(scanArgs...)
		row_map := make(map[string]interface{})
		for i, value := range values {
			row_map[columns[i].Name()] = bytes2RealType(value, columns[i])
		}
		rows_map = append(rows_map, row_map)
	}
	if err = rows.Err(); err != nil {
		return []map[string]interface{}{}, err
	}
	return rows_map, nil
}

func QueryTx(conn *sql.Tx, query string, args ...interface{}) ([]map[string]interface{}, error) {
	rows, err := conn.Query(query, args...)
	if err != nil {
		return []map[string]interface{}{}, err
	}
	defer rows.Close()
	// 返回属性字典
	columns, err := mysqlinternals.Columns(rows)
	if err != nil {
		return []map[string]interface{}{}, err
	}
	// 获取字段类型
	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(columns))
	for i, _ := range values {
		scanArgs[i] = &values[i]
	}
	rows_map := make([]map[string]interface{}, 0, 10)
	for rows.Next() {
		rows.Scan(scanArgs...)
		row_map := make(map[string]interface{})
		for i, value := range values {
			row_map[columns[i].Name()] = bytes2RealType(value, columns[i])
		}
		rows_map = append(rows_map, row_map)
	}
	if err = rows.Err(); err != nil {
		return []map[string]interface{}{}, err
	}
	return rows_map, nil
}

func Query(conn interface{}, query string, args ...interface{}) ([]map[string]interface{}, error) {
	switch conn.(type) {
	case *sql.DB:
		obj, _ := conn.(*sql.DB)
		if err := obj.Ping(); err != nil {
			return []map[string]interface{}{}, err
		}
		return QueryDB(obj, query, args...)
	case *sql.Tx:
		obj, _ := conn.(*sql.Tx)
		return QueryTx(obj, query, args...)
	}

	return []map[string]interface{}{}, nil
}

func QueryOne(conn interface{}, query string, args ...interface{}) (map[string]interface{}, error) {
	switch conn.(type) {
	case *sql.DB:
		obj, _ := conn.(*sql.DB)
		if err := obj.Ping(); err != nil {
			return nil, err
		}
		rows, err := QueryDB(obj, query, args...)
		if len(rows) == 0 {
			return nil, err
		}
		if len(rows) > 1 {
			return nil, errors.New("QueryOne函数调用，出现多条数据")
		}

		return rows[0], err

	case *sql.Tx:
		obj, _ := conn.(*sql.Tx)
		rows, err := QueryTx(obj, query, args...)
		if len(rows) == 0 {
			return nil, err
		}
		if len(rows) > 1 {
			return nil, errors.New("QueryOne函数调用，出现多条数据")
		}
		return rows[0], err
	}

	return nil, nil
}

func execute(conn interface{}, update string, args ...interface{}) (sql.Result, error) {
	switch conn.(type) {
	case *sql.DB:
		obj, _ := conn.(*sql.DB)
		if ping_err := obj.Ping(); ping_err != nil {
			return nil, ping_err
		}
		return obj.Exec(update, args...)
	case *sql.Tx:
		obj, _ := conn.(*sql.Tx)
		return obj.Exec(update, args...)
	}

	return nil, nil
}

func Update(conn interface{}, update string, args ...interface{}) (int64, error) {

	result, err := execute(conn, update, args...)
	if err != nil {
		return 0, err
	}
	affect, err := result.RowsAffected()
	return affect, err
}

func Insert(conn interface{}, insert string, args ...interface{}) (int64, error) {
	result, err := execute(conn, insert, args...)
	if err != nil {
		return 0, err
	}
	lastid, err := result.LastInsertId()
	return lastid, err

}

func Delete(conn interface{}, delete string, args ...interface{}) (int64, error) {
	result, err := execute(conn, delete, args...)
	if err != nil {
		return 0, err
	}
	affect, err := result.RowsAffected()
	return affect, err
}

func bytes2RealType(src []byte, column mysqlinternals.Column) interface{} {
	srcStr := string(src)
	var result interface{}
	switch column.MysqlType() {
	case "TINYINT":
		fallthrough
	case "SMALLINT":
		fallthrough
	case "INT":
		result, _ = strconv.ParseInt(srcStr, 10, 64)
	case "BIGINT":
		if column.IsUnsigned() {
			result, _ = strconv.ParseUint(srcStr, 10, 64)
		} else {
			result, _ = strconv.ParseInt(srcStr, 10, 64)
		}
	case "CHAR":
		fallthrough
	case "VARCHAR":
		fallthrough
	case "BLOB":
		fallthrough
	case "TIMESTAMP":
		fallthrough
	case "DATE":
		fallthrough
	case "DATETIME":
		fallthrough
	case "TIME":
		result = srcStr
	case "FLOAT":
		fallthrough
	case "DOUBLE":
		result, _ = strconv.ParseFloat(srcStr, 32)
	case "DECIMAL":
		fee, _ := decimal.NewFromString(srcStr)
		result, _ = fee.Float64()
	default:
		result = nil
	}
	return result
}
