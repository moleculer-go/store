package sqlite

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"strconv"

	"github.com/moleculer-go/moleculer/payload"
	"go.mongodb.org/mongo-driver/bson"

	"github.com/moleculer-go/moleculer"

	"crawshaw.io/sqlite"
	"crawshaw.io/sqlite/sqlitex"
	log "github.com/sirupsen/logrus"
)

type Column struct {
	Name string
	Type string
}

type SQLiteAdapter struct {
	URI      string
	Flags    sqlite.OpenFlags
	PoolSize int
	Timeout  time.Duration
	Table    string
	Columns  []Column
	// ColName can be used to modify/translate column names
	// from what is passed in the params
	ColName func(string) string

	pool     *sqlitex.Pool
	log      *log.Entry
	settings map[string]interface{}

	fields  []string
	idField string
}

func (a *SQLiteAdapter) Init(log *log.Entry, settings map[string]interface{}) {
	a.log = log
	a.settings = settings
	if a.Timeout == 0 {
		a.Timeout = time.Second * 2
	}
	if a.ColName == nil {
		a.ColName = func(value string) string {
			return value
		}
	}
	a.loadSettings(a.settings)
}

func (a *SQLiteAdapter) Connect() error {
	pool, err := sqlitex.Open(a.URI, a.Flags, a.PoolSize)
	if err != nil {
		a.log.Error("Could not connect to SQLite - error: ", err)
		return errors.New(fmt.Sprint("Could not connect to SQLite - error: ", err))
	}
	a.pool = pool
	err = a.createTable()
	if err != nil {
		a.log.Error("Could not create table - error: ", err)
		return errors.New(fmt.Sprint("Could not create table - error: ", err))
	}
	return nil
}

func (a *SQLiteAdapter) columnsDefinition() []string {
	columns := []string{a.idField + " INTEGER PRIMARY KEY AUTOINCREMENT"}
	for _, c := range a.Columns {
		def := c.Name
		if c.Type != "" {
			def = def + " " + c.Type
		}
		columns = append(columns, def)
	}
	return columns
}

func (a *SQLiteAdapter) createTable() error {
	conn := a.getConn()
	if conn == nil {
		return noConnectionError().Error()
	}
	defer a.returnConn(conn)

	create := "CREATE TABLE " + a.Table + " (" + strings.Join(a.columnsDefinition(), ", ") + ");"
	a.log.Debug(create)

	err := sqlitex.ExecTransient(conn, create, nil)
	if err != nil {
		return err
	}
	a.log.Debug("table " + a.Table + " created !!!")
	return nil
}

func (a *SQLiteAdapter) Disconnect() error {
	err := a.pool.Close()
	if err != nil {
		a.log.Error("Could not disconnect SQLite - error: ", err)
		return errors.New(fmt.Sprint("Could not disconnect SQLite - error: ", err))
	}
	return nil
}

func noConnectionError() moleculer.Payload {
	return payload.Error("No connection availble!. Did you call adapter.Connect() ?")
}

func (a *SQLiteAdapter) returnConn(conn *sqlite.Conn) {
	a.pool.Put(conn)
}

func (a *SQLiteAdapter) getConn() *sqlite.Conn {
	return a.pool.Get(nil)
}

// extractFields will parse the payload and extract the column names,
// and value placeholders -> $name and the list of fields.
func (a *SQLiteAdapter) insertFields(param moleculer.Payload) ([]string, []interface{}) {
	columns := []string{}
	values := []interface{}{}
	param.ForEach(func(key interface{}, value moleculer.Payload) bool {
		col, ok := key.(string)
		if !ok {
			a.log.Error("extractFields() key must be string! - key: ", key)
			return false
		}
		columns = append(columns, a.ColName(col))
		values = append(values, value.Value())
		return true
	})
	return columns, values
}

func (a *SQLiteAdapter) populateStmt(stmt *sqlite.Stmt, param moleculer.Payload, fields []string) (err error) {
	param.ForEach(func(key interface{}, value moleculer.Payload) bool {
		field, ok := key.(string)
		if !ok {
			a.log.Error("populateStmt() key must be string! - key: ", key)
			err = errors.New(fmt.Sprint("populateStmt() key must be string! - key: ", key))
			return false
		}
		stmt.SetText("$"+field, value.String())
		return true
	})
	return err
}

func placeholders(c []string) []string {
	p := make([]string, len(c))
	for i, _ := range c {
		p[i] = "?"
	}
	return p
}

func (a *SQLiteAdapter) loadSettings(settings map[string]interface{}) {
	idField, hasIdField := settings["idField"].(string)
	if !hasIdField {
		idField = "id"
	}

	fields, hasFields := settings["fields"].([]string)
	if !hasFields {
		fields = []string{}
		for _, c := range a.Columns {
			fields = append(fields, c.Name)
		}
	}
	fields = append(fields, idField)

	a.fields = fields
	a.idField = idField
}

func (a *SQLiteAdapter) Insert(param moleculer.Payload) moleculer.Payload {
	conn := a.getConn()
	if conn == nil {
		return noConnectionError()
	}
	defer a.returnConn(conn)

	columns, values := a.insertFields(param)
	insert := "INSERT INTO " + a.Table + " (" + strings.Join(columns, ", ") + ") VALUES(" + strings.Join(placeholders(columns), ", ") + ") ;"
	if err := sqlitex.Exec(conn, insert, nil, values...); err != nil {
		a.log.Error("Error on insert: ", err)
		return payload.New(err)
	}
	return param.Add(a.idField, conn.LastInsertRowID())
}

func (a *SQLiteAdapter) RemoveById(id moleculer.Payload) moleculer.Payload {
	conn := a.getConn()
	if conn == nil {
		return noConnectionError()
	}
	defer a.returnConn(conn)

	delete := "DELETE FROM " + a.Table + " WHERE id = " + id.String() + " ;"
	a.log.Debug(delete)
	if err := sqlitex.Exec(conn, delete, nil); err != nil {
		a.log.Error("Error on delete: ", err)
		return payload.New(err)
	}
	deletedCount := conn.Changes()
	return payload.Empty().Add("deletedCount", deletedCount)
}

func resolveFields(fields []string, param moleculer.Payload) []string {
	if param.Get("fields").Exists() && param.Get("fields").IsArray() {
		fields = param.Get("fields").StringArray()
	}
	return fields
}

func (adapter *SQLiteAdapter) FindById(id moleculer.Payload) moleculer.Payload {
	filter := payload.New(map[string]interface{}{
		"query": map[string]interface{}{adapter.idField: id.Value()},
	})
	return adapter.FindOne(filter)
}

func (adapter *SQLiteAdapter) FindByIds(params moleculer.Payload) moleculer.Payload {
	if !params.IsArray() {
		return payload.Error("FindByIds() only support lists!  --> !params.IsArray()")
	}
	r := payload.EmptyList()
	params.ForEach(func(idx interface{}, id moleculer.Payload) bool {
		r = r.AddItem(adapter.FindById(id))
		return true
	})
	return r
}

func (a *SQLiteAdapter) FindOne(params moleculer.Payload) moleculer.Payload {
	params = params.Add("limit", 1)
	return a.Find(params).First()
}

func (a *SQLiteAdapter) Find(param moleculer.Payload) moleculer.Payload {
	conn := a.getConn()
	if conn == nil {
		return noConnectionError()
	}
	defer a.returnConn(conn)

	fields := resolveFields(a.fields, param)

	rows := []moleculer.Payload{}
	where := strings.Join(a.parseFilter(param), ", ")
	selec := "SELECT " + strings.Join(fields, ", ") + " FROM " + a.Table + " WHERE " + where + " ;"
	a.log.Debug(selec)
	if err := sqlitex.Exec(conn, selec, func(stmt *sqlite.Stmt) error {
		rows = append(rows, a.rowToPayload(fields, stmt))
		return nil
	}); err != nil {
		a.log.Error("Error on select: ", err)
		return payload.New(err)
	}
	return payload.New(rows)
}

func (a *SQLiteAdapter) columnValue(column string, stmt *sqlite.Stmt) interface{} {
	t := a.columnType(column)
	if t == "NUMBER" {
		return stmt.GetFloat(column)
	}
	if t == "INTEGER" {
		return stmt.GetInt64(column)
	}
	return stmt.GetText(column)
}

func (a *SQLiteAdapter) rowToPayload(fields []string, stmt *sqlite.Stmt) moleculer.Payload {
	data := map[string]interface{}{}
	for _, c := range fields {
		data[c] = a.columnValue(c, stmt)
	}
	return payload.New(data)
}

func (a *SQLiteAdapter) columnType(field string) (r string) {
	for _, c := range a.Columns {
		if c.Name == field {
			return c.Type
		}
	}
	return r
}

func (a *SQLiteAdapter) wrapValue(cType string, value moleculer.Payload) (r string) {
	if cType == "TEXT" || cType == "" {
		return "'" + value.String() + "'"
	}
	if cType == "NUMBER" {
		return "'" + strconv.FormatFloat(value.Float(), 'f', 6, 64) + "'"
	}
	if cType == "INTEGER" {
		return "'" + strconv.FormatInt(value.Int64(), 64) + "'"
	}

	return r
}

func (a *SQLiteAdapter) where(query moleculer.Payload) (pairs []string) {
	query.ForEach(func(key interface{}, value moleculer.Payload) bool {
		field := key.(string)
		v := a.wrapValue(a.columnType(field), value)
		pairs = append(pairs, field+" = "+v)
		return true
	})
	return pairs
}

func (a *SQLiteAdapter) parseFilter(params moleculer.Payload) []string {
	query := payload.Empty()
	if params.Get("query").Exists() {
		query = params.Get("query")
	}
	query = parseSearchFields(params, query)
	return a.where(query)
}

func parseSearchFields(params, query moleculer.Payload) moleculer.Payload {
	searchFields := params.Get("searchFields")
	search := params.Get("search")
	searchValue := ""
	if search.Exists() {
		searchValue = search.String()
	}
	if searchFields.Exists() {
		fields := searchFields.StringArray()
		if len(fields) == 1 {
			query = query.Add(fields[0], searchValue)
		} else if len(fields) > 1 {
			or := payload.EmptyList()
			for _, field := range fields {
				bm := bson.M{}
				bm[field] = searchValue
				or = or.AddItem(bm)
			}
			query = query.Add("$or", or)
		}
	}
	return query
}

// func parseFindOptions(params moleculer.Payload) *options.FindOptions {
// 	opts := options.FindOptions{}
// 	limit := params.Get("limit")
// 	offset := params.Get("offset")
// 	sort := params.Get("sort")
// 	if limit.Exists() {
// 		v := limit.Int64()
// 		opts.Limit = &v
// 	}
// 	if offset.Exists() {
// 		v := offset.Int64()
// 		opts.Skip = &v
// 	}
// 	if sort.Exists() {
// 		if sort.IsArray() {
// 			opts.Sort = sortsFromStringArray(sort)
// 		} else {
// 			opts.Sort = sortsFromString(sort)
// 		}

// 	}
// 	return &opts
// }
