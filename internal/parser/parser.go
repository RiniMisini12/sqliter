package parser

import (
	"errors"
	"strings"
)

type Statement interface{}

type InsertStatement struct {
	Table  string
	Values []string
}

type SelectStatement struct {
	Table string
}

type UpdateStatement struct {
	Table  string
	Values []string
}

type DeleteStatement struct {
	Table string
}

func ParseSQL(sql string) (Statement, error) {
	tokens := strings.Fields(sql)
	if len(tokens) == 0 {
		return nil, errors.New("empty SQL statement")
	}

	keyword := strings.ToUpper(tokens[0])
	switch keyword {
	case "INSERT":
		if len(tokens) < 5 {
			return nil, errors.New("invalid INSERT statement: too few tokens")
		}
		if strings.ToUpper(tokens[1]) != "INTO" {
			return nil, errors.New("invalid INSERT statement: missing INTO")
		}
		tableName := tokens[2]
		if strings.ToUpper(tokens[3]) != "VALUES" {
			return nil, errors.New("invalid INSERT statement: missing VALUES")
		}
		var values []string
		for _, token := range tokens[4:] {
			token = strings.Trim(token, ",")
			values = append(values, token)
		}
		return &InsertStatement{
			Table:  tableName,
			Values: values,
		}, nil

	case "SELECT":
		if len(tokens) < 4 {
			return nil, errors.New("invalid SELECT statement: too few tokens")
		}
		if tokens[1] != "*" {
			return nil, errors.New("only SELECT * is supported")
		}
		if strings.ToUpper(tokens[2]) != "FROM" {
			return nil, errors.New("invalid SELECT statement: missing FROM")
		}
		tableName := tokens[3]
		return &SelectStatement{
			Table: tableName,
		}, nil
	case "UPDATE":
		if len(tokens) < 5 {
			return nil, errors.New("invalid UPDATE statement: too few tokens")
		}
		tableName := tokens[1]
		if strings.ToUpper(tokens[2]) != "SET" {
			return nil, errors.New("invalid UPDATE statement: missing SET")
		}
		var values []string
		for _, token := range tokens[3:] {
			token = strings.Trim(token, ",")
			values = append(values, token)
		}
		return &UpdateStatement{
			Table:  tableName,
			Values: values,
		}, nil
	case "DELETE":
		if len(tokens) < 4 {
			return nil, errors.New("invalid DELETE statement: too few tokens")
		}
		if strings.ToUpper(tokens[1]) != "FROM" {
			return nil, errors.New("invalid DELETE statement: missing FROM")
		}
		tableName := tokens[2]
		return &DeleteStatement{
			Table: tableName,
		}, nil
	default:
		return nil, errors.New("unsupported SQL statement")
	}
}
