package executor

import (
	"errors"
	"fmt"

	"github.com/rinimisini112/sqliter/internal/engine"
	"github.com/rinimisini112/sqliter/internal/parser"
	"github.com/rinimisini112/sqliter/internal/planner"
	"github.com/rinimisini112/sqliter/internal/storage"
)

type Executor struct {
	engine *engine.Engine
}

func NewExecutor(engine *engine.Engine) *Executor {
	return &Executor{
		engine: engine,
	}
}

func (ex *Executor) ExecutePlan(plan *planner.QueryPlan) (ResultIterator, error) {
	switch plan.Operation {
	case "INSERT":
		row := &storage.Row{
			Values: make([]interface{}, len(plan.Values)),
		}
		for i, val := range plan.Values {
			row.Values[i] = val
		}
		table, err := engine.NewTable(ex.engine)
		if err != nil {
			return nil, err
		}
		if err := table.InsertRow(row); err != nil {
			return nil, err
		}
		return &SimpleRowIterator{
			rows:  []*storage.Row{},
			index: 0,
		}, nil

	case "SELECT":
		table, err := engine.NewTable(ex.engine)
		if err != nil {
			return nil, err
		}
		rows, err := table.GetRows()
		if err != nil {
			return nil, err
		}
		// (Optionally, apply filtering, sorting, or grouping here.)
		return &SimpleRowIterator{
			rows:  rows,
			index: 0,
		}, nil

	default:
		return nil, errors.New("unsupported operation")
	}
}

func ExecuteSQL(dbPath, sql string) error {
	engine, err := engine.NewEngine(dbPath)
	if err != nil {
		return fmt.Errorf("engine initialization error: %w", err)
	}
	defer engine.Close()

	stmt, err := parser.ParseSQL(sql)
	if err != nil {
		return fmt.Errorf("parse error: %w", err)
	}

	plan, err := planner.PlanQuery(stmt)
	if err != nil {
		return fmt.Errorf("planning error: %w", err)
	}

	exec := NewExecutor(engine)
	resultIterator, err := exec.ExecutePlan(plan)
	if err != nil {
		return fmt.Errorf("execution error: %w", err)
	}

	if plan.Operation == "SELECT" {
		fmt.Println("Query results:")
		for {
			row, err := resultIterator.Next()
			if err != nil {
				break
			}
			fmt.Println(row.Values)
		}
	} else if plan.Operation == "INSERT" {
		fmt.Println("Row inserted successfully.")
	}

	return nil
}
