package mysql

import (
	"fmt"
	"strings"
)

// 插入表数据
func InsertTR(db interface{}, table string, info map[string]interface{}) (int64, error) {
	var (
		columns []string
		symbols []string
		args    []interface{}
	)
	for k, v := range info {
		columns = append(columns, k)
		symbols = append(symbols, "?")
		args = append(args, v)
	}
	sql := fmt.Sprintf(`INSERT INTO %v(%v) VALUES(%v)`, table, strings.Join(columns, ","), strings.Join(symbols, ","))
	return Insert(db, sql, args...)
}

// 删除表数据
func DeleteTR(db interface{}, table string, condition map[string]interface{}) (int64, error) {
	var (
		columns []string
		args    []interface{}
	)
	for k, v := range condition {
		columns = append(columns, k+"=?")
		args = append(args, v)
	}
	sql := fmt.Sprintf(`DELETE FROM %v WHERE %v `, table, strings.Join(columns, " AND "))
	return Delete(db, sql, args...)
}

// 更新表数据
func UpdateTR(db interface{}, table string, condition, info map[string]interface{}) (int64, error) {
	var (
		columns []string
		args    []interface{}
	)
	for k, v := range info {
		columns = append(columns, k+"=?")
		args = append(args, v)
	}
	conds := []string{}
	for k, v := range condition {
		conds = append(conds, k+"=?")
		args = append(args, v)
	}
	sql := fmt.Sprintf(`UPDATE %v SET %v WHERE %v`, table, strings.Join(columns, ","), strings.Join(conds, " AND "))
	return Update(db, sql, args...)
}

// 获取多条记录，条件查询
func GetTRInfo(db interface{}, table string, info map[string]interface{}, limit ...interface{}) ([]map[string]interface{}, error) {
	if len(info) == 0 {
		info = map[string]interface{}{
			"1": 1,
		}
	}
	columns := []string{}
	args := []interface{}{}
	for k, v := range info {
		columns = append(columns, k+"=?")
		args = append(args, v)
	}
	sql := ""
	if len(limit) > 0 {
		limitStr := []string{}
		for _, l := range limit {
			limitStr = append(limitStr, fmt.Sprintf("%v", l))
		}
		sql = fmt.Sprintf(`SELECT * FROM %v WHERE %v ORDER BY id  ASC LIMIT %v`, table, strings.Join(columns, " AND "), strings.Join(limitStr, ","))
	} else {
		sql = fmt.Sprintf(`SELECT * FROM %v WHERE %v ORDER BY id  ASC `, table, strings.Join(columns, " AND "))
	}
	return Query(db, sql, args...)
}

// 获取一条记录，条件查询
func GetOneTRInfo(db interface{}, table string, info map[string]interface{}) (map[string]interface{}, error) {
	rows, err := GetTRInfo(db, table, info, 1)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return rows[0], nil
}

// 获取指定字段返回记录，条件查询
func GetOneTRInfoForColumns(db interface{}, table string, columns []string, info map[string]interface{}) (map[string]interface{}, error) {
	if len(info) == 0 {
		info = map[string]interface{}{
			"1": 1,
		}
	}
	columnsTemp := []string{}
	args := []interface{}{}
	for k, v := range info {
		columnsTemp = append(columnsTemp, k+"=?")
		args = append(args, v)
	}
	sql := fmt.Sprintf(`SELECT %v FROM %v WHERE %v`, strings.Join(columns, ","), table, strings.Join(columnsTemp, " AND "))

	rows, err := Query(db, sql, args...)

	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return rows[0], nil
}

// 返回符合条件的记录条数
func CountTR(db interface{}, table string, info map[string]interface{}) (int64, error) {
	if len(info) == 0 {
		info = map[string]interface{}{
			"1": 1,
		}
	}
	columns := []string{}
	args := []interface{}{}
	for k, v := range info {
		columns = append(columns, k+"=?")
		args = append(args, v)
	}
	sql := fmt.Sprintf("SELECT COUNT(*) AS num FROM %v WHERE %v", table, strings.Join(columns, " AND "))
	info, err := QueryOne(db, sql, args...)
	if info != nil {
		return info["num"].(int64), err
	}
	return 0, err
}
