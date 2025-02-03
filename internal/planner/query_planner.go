package planner

import (
	"fmt"

	"github.com/rinimisini112/sqliter/internal/parser"
)

type QueryPlan struct {
	Operation string
	Table     string
	Values    []string
}

func PlanQuery(stmt parser.Statement) (*QueryPlan, error) {
	switch s := stmt.(type) {
	case *parser.InsertStatement:
		return &QueryPlan{
			Operation: "INSERT",
			Table:     s.Table,
			Values:    s.Values,
		}, nil
	case *parser.SelectStatement:
		return &QueryPlan{
			Operation: "SELECT",
			Table:     s.Table,
		}, nil
	case *parser.UpdateStatement:
		return &QueryPlan{
			Operation: "UPDATE",
			Table:     s.Table,
			Values:    s.Values,
		}, nil
	case *parser.DeleteStatement:
		return &QueryPlan{
			Operation: "DELETE",
			Table:     s.Table,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported statement type in planner")
	}
}
