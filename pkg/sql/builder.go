package sql

import (
	"fmt"
	"strings"
)

// BuildInsertQuery creates an INSERT SQL query with placeholders for MySQL
func BuildInsertQuery(table string, values map[string]interface{}) (string, []interface{}) {
	var cols []string
	var placeholders []string
	var args []interface{}
	for k, v := range values {
		cols = append(cols, fmt.Sprintf("`%s`", k))
		placeholders = append(placeholders, "?")
		args = append(args, v)
	}
	query := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s)", table, strings.Join(cols, ", "), strings.Join(placeholders, ", "))
	// Optionally: use ON DUPLICATE KEY UPDATE for idempotency
	// query += " ON DUPLICATE KEY UPDATE " + ...
	return query, args
}

// BuildUpdateQuery creates an UPDATE SQL query with placeholders for MySQL
func BuildUpdateQuery(table string, values map[string]interface{}, pkColumn string, pkValue interface{}) (string, []interface{}) {
	var setClauses []string
	var args []interface{}
	for k, v := range values {
		if k != pkColumn { // Do not update the primary key
			setClauses = append(setClauses, fmt.Sprintf("`%s` = ?", k))
			args = append(args, v)
		}
	}
	args = append(args, pkValue)
	query := fmt.Sprintf("UPDATE `%s` SET %s WHERE `%s` = ?", table, strings.Join(setClauses, ", "), pkColumn)
	return query, args
}

// BuildDeleteQuery creates a DELETE SQL query with placeholders for MySQL
func BuildDeleteQuery(table string, pkColumn string, pkValue interface{}) (string, []interface{}) {
	query := fmt.Sprintf("DELETE FROM `%s` WHERE `%s` = ?", table, pkColumn)
	return query, []interface{}{pkValue}
}
