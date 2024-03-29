package database

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/sqls-server/sqls/dialect"
	"github.com/sqls-server/sqls/parser/parseutil"
)

var (
	ErrNotImplementation error = errors.New("not implementation")
)

const (
	DefaultMaxIdleConns = 10
	DefaultMaxOpenConns = 5
)

type DBRepository interface {
	Driver() dialect.DatabaseDriver
	CurrentDatabase(ctx context.Context) (string, error)
	Databases(ctx context.Context) ([]string, error)
	CurrentSchema(ctx context.Context) (string, error)
	Schemas(ctx context.Context) ([]string, error)
	SchemaTables(ctx context.Context) (map[string][]string, error)
	DescribeDatabaseTable(ctx context.Context) ([]*ColumnDesc, error)
	DescribeDatabaseTableBySchema(ctx context.Context, schemaName string) ([]*ColumnDesc, error)
	Exec(ctx context.Context, query string) (sql.Result, error)
	Query(ctx context.Context, query string) (*sql.Rows, error)
	DescribeForeignKeysBySchema(ctx context.Context, schemaName string) ([]*ForeignKey, error)
}

type DBOption struct {
	MaxIdleConns int
	MaxOpenConns int
}

type ColumnBase struct {
	Schema string
	Table  string
	Name   string
}

type ColumnDesc struct {
	ColumnBase
	Type    string
	Null    string
	Key     string
	Default sql.NullString
	Extra   string
}

type ForeignKey [][2]*ColumnBase

type fkItemDesc struct {
	fkID      string
	schema    string
	table     string
	column    string
	refTable  string
	refColumn string
	refSchema string
}

func (cd *ColumnDesc) OnelineDesc() string {
	items := []string{}
	if cd.Type != "" {
		items = append(items, "`"+cd.Type+"`")
	}
	if cd.Key == "YES" {
		items = append(items, "PRIMARY KEY")
	} else if cd.Key != "" && cd.Key != "NO" {
		items = append(items, cd.Key)
	}
	if cd.Extra != "" {
		items = append(items, cd.Extra)
	}
	return strings.Join(items, " ")
}

func ColumnDoc(tableName string, colDesc *ColumnDesc) string {
	buf := new(bytes.Buffer)
	fmt.Fprintf(buf, "`%s`.`%s` column", tableName, colDesc.Name)
	fmt.Fprintln(buf)
	fmt.Fprintln(buf)
	fmt.Fprintln(buf, colDesc.OnelineDesc())
	return buf.String()
}

func Coalesce(str ...string) string {
	for _, s := range str {
		if s != "" {
			return s
		}
	}
	return ""
}

func cellFormat(str ...string) []string {
	result := []string{}
	for _, s := range str {
		if s != "" {
			result = append(result, fmt.Sprintf("`%s`", s))
		} else {
			result = append(result, "")
		}

	}
	return result
}

func TableDoc(tableName string, cols []*ColumnDesc) string {
	buf := new(bytes.Buffer)
	fmt.Fprintf(buf, "# `%s` table", tableName)
	fmt.Fprintln(buf)
	fmt.Fprintln(buf)
	fmt.Fprintln(buf)

	columnSizes := [5]int{20, 30, 15, 20, 20}

	rowFormat := fmt.Sprintf( "| %%-%ds | %%-%ds | %%-%ds | %%-%ds | %%-%ds |\n", columnSizes[0], columnSizes[1], columnSizes[2], columnSizes[3], columnSizes[4])

	fmt.Fprintf(buf, rowFormat, "Name", "Type", "Primary key", "Default", "Extra")

	var headerSeparators [5]string
	for i, size := range columnSizes {
		headerSeparators[i] = ":"+strings.Repeat("-", size - 1)
	}
	fmt.Fprintf(buf, rowFormat, headerSeparators[0], headerSeparators[1], headerSeparators[2], headerSeparators[3], headerSeparators[4])

	for _, col := range cols {
		cellContent := cellFormat(col.Name, col.Type, col.Key, Coalesce(col.Default.String, "-"), col.Extra)
		fmt.Fprintf(buf, rowFormat, cellContent[0], cellContent[1], cellContent[2], cellContent[3], cellContent[4])
	}
	return buf.String()
}

func SubqueryDoc(name string, views []*parseutil.SubQueryView, dbCache *DBCache) string {
	buf := new(bytes.Buffer)
	fmt.Fprintf(buf, "%s subquery", name)
	fmt.Fprintln(buf)
	fmt.Fprintln(buf)
	for _, view := range views {
		for _, colmun := range view.SubQueryColumns {
			if colmun.ColumnName == "*" {
				tableCols, ok := dbCache.ColumnDescs(colmun.ParentTable.Name)
				if !ok {
					continue
				}
				for _, tableCol := range tableCols {
					fmt.Fprintf(buf, "- %s(%s.%s): %s", tableCol.Name, colmun.ParentTable.Name, tableCol.Name, tableCol.OnelineDesc())
					fmt.Fprintln(buf)
				}
			} else {
				columnDesc, ok := dbCache.Column(colmun.ParentTable.Name, colmun.ColumnName)
				if !ok {
					continue
				}
				fmt.Fprintf(buf, "- %s(%s.%s): %s", colmun.DisplayName(), colmun.ParentTable.Name, colmun.ColumnName, columnDesc.OnelineDesc())
				fmt.Fprintln(buf)

			}
		}
	}
	return buf.String()
}

func SubqueryColumnDoc(identName string, views []*parseutil.SubQueryView, dbCache *DBCache) string {
	buf := new(bytes.Buffer)
	fmt.Fprintf(buf, "%s subquery column", identName)
	fmt.Fprintln(buf)
	fmt.Fprintln(buf)
	for _, view := range views {
		for _, colmun := range view.SubQueryColumns {
			if colmun.ColumnName == "*" {
				tableCols, ok := dbCache.ColumnDescs(colmun.ParentTable.Name)
				if !ok {
					continue
				}
				for _, tableCol := range tableCols {
					if identName == tableCol.Name {
						fmt.Fprintf(buf, "- %s(%s.%s): %s", identName, colmun.ParentTable.Name, tableCol.Name, tableCol.OnelineDesc())
						fmt.Fprintln(buf)
						continue
					}
				}
			} else {
				if identName != colmun.ColumnName && identName != colmun.AliasName {
					continue
				}
				columnDesc, ok := dbCache.Column(colmun.ParentTable.Name, colmun.ColumnName)
				if !ok {
					continue
				}
				fmt.Fprintf(buf, "- %s(%s.%s): %s", identName, colmun.ParentTable.Name, colmun.ColumnName, columnDesc.OnelineDesc())
				fmt.Fprintln(buf)
			}
		}
	}
	return buf.String()
}

func parseForeignKeys(rows *sql.Rows, schemaName string) ([]*ForeignKey, error) {
	columns, err := rows.Columns()

	if err != nil {
		return nil, err
	}

	var retVal []*ForeignKey
	var prevFk, prevTable string
	var cur *ForeignKey
	for rows.Next() {
		var fkItem fkItemDesc
		if len(columns) == 6 {
			err = rows.Scan(
				&fkItem.fkID,
				&fkItem.table,
				&fkItem.column,
				&fkItem.refTable,
				&fkItem.refColumn,
				&fkItem.refSchema,
			)
		} else {
			err = rows.Scan(
				&fkItem.fkID,
				&fkItem.table,
				&fkItem.column,
				&fkItem.refTable,
				&fkItem.refColumn,
			)
			fkItem.refSchema = schemaName
		}
		if err != nil {
			return nil, err
		}
		var l, r ColumnBase
		l.Schema = schemaName
		l.Table = fkItem.table
		l.Name = fkItem.column
		r.Schema = fkItem.refSchema
		r.Table = fkItem.refTable
		r.Name = fkItem.refColumn
		if fkItem.fkID != prevFk || fkItem.table != prevTable {
			if cur != nil {
				retVal = append(retVal, cur)
			}
			cur = new(ForeignKey)
		}
		*cur = append(*cur, [2]*ColumnBase{&l, &r})
		prevFk = fkItem.fkID
		prevTable = fkItem.table
	}

	if cur != nil {
		retVal = append(retVal, cur)
	}
	return retVal, nil
}
