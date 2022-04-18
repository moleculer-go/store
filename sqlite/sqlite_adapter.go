package sqlite

import (
	"errors"
	"fmt"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"strconv"

	"github.com/moleculer-go/moleculer/payload"
	"github.com/moleculer-go/moleculer/serializer"

	"github.com/moleculer-go/moleculer"

	"crawshaw.io/sqlite"
	"crawshaw.io/sqlite/sqlitex"
	log "github.com/sirupsen/logrus"
)

const (
	//YYYY-MM-DD HH:MM:SS.SSS -> SQLIte format
	ISO8601 = "2006-01-02 15:04:05.000"
)

type Column struct {
	Name string
	Type string
}

type Adapter struct {
	URI      string
	Flags    sqlite.OpenFlags
	PoolSize int
	Timeout  time.Duration
	Table    string
	Columns  []Column
	// ColName can be used to modify/translate column names
	// from what is passed in the params
	ColName func(string) string

	pool                 *sqlitex.Pool
	waitForPoolLimit     time.Duration
	connInUse            int
	waitConnectionsLimit time.Duration

	connected bool
	log       *log.Entry
	settings  map[string]interface{}

	fields     []string
	idField    string
	idColumn   *Column
	serializer serializer.Serializer
}

func (a *Adapter) Init(log *log.Entry, settings map[string]interface{}) {
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
	if a.PoolSize == 0 {
		a.PoolSize = 1
	}
	a.waitForPoolLimit = time.Millisecond * 500
	a.waitConnectionsLimit = time.Second * 2
	a.loadSettings(a.settings)
	a.serializer = serializer.CreateJSONSerializer(a.log)
}

var pools map[string]*sqlitex.Pool
var poolsMutex = &sync.Mutex{}

// Experiment in progress...
func (a *Adapter) poolFromCache() (*sqlitex.Pool, error) {
	poolsMutex.Lock()
	defer poolsMutex.Unlock()

	if pools == nil {
		pools = map[string]*sqlitex.Pool{}
	}

	pool, exists := pools[a.URI]
	if !exists {
		var err error
		pool, err = sqlitex.Open(a.URI, a.Flags, a.PoolSize)
		if err == nil {
			pools[a.URI] = pool
		}
		fmt.Println("** new pool **")
		return pool, err
	}
	fmt.Println("** pool from cache **")
	return pool, nil
}

func (a *Adapter) Connect() error {
	if a.connected {
		return nil
	}
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
	a.log.Info("SQLite adapter " + a.Table + " connected!")
	a.connected = true
	return nil
}

// waitConnections wait for all connections to be returned to the pool
func (a *Adapter) waitConnections() error {
	start := time.Now()
	for {
		if a.connInUse == 0 {
			return nil
		}
		if a.waitConnectionsLimit != 0 && time.Since(start) >= a.waitConnectionsLimit {
			return errors.New("waitConnections() timeout! There are still " + strconv.Itoa(a.connInUse) + " connections in use.")
		}
		time.Sleep(time.Microsecond)
	}
}

func (a *Adapter) Disconnect() error {
	if !a.connected {
		return nil
	}
	a.log.Info("SQLite adapter - waiting for connections...")
	err := a.waitConnections()
	if err != nil {
		a.log.Error("Could not disconnect SQLite - error: ", err)
		defer func() {
			a.waitConnections()
			a.pool.Close()
		}()
		return errors.New(fmt.Sprint("Could not disconnect SQLite - error: ", err))
	}
	a.log.Info("SQLite adapter - all connections were returned :) - closing pool now.")
	err = a.pool.Close()
	if err != nil {
		a.log.Error("Could not disconnect SQLite - error: ", err)
		return errors.New(fmt.Sprint("Could not disconnect SQLite - error: ", err))
	}
	a.pool = nil
	a.connected = false
	return nil
}

func noConnectionError() moleculer.Payload {
	return payload.Error("No connection availble!. Did you call a.Connect() ?")
}

func (a *Adapter) catchConnError(msg string, resChan chan moleculer.Payload) {
	if err := recover(); err != nil {
		stackTrace := string(debug.Stack())
		a.log.Error("SQLite adapter Error - message: ", msg, " - error: ", err, " stack track: ", stackTrace)
		resChan <- payload.New(err)
	}
}

func (a *Adapter) returnConn(conn *sqlite.Conn) {
	a.pool.Put(conn)
	a.connInUse = a.connInUse - 1
}

// getConn fetch a connection from the pool
// if pool is not available and setting waitForPoolLimit is set
// it will wait for that period for the pool to be available
func (a *Adapter) getConn() *sqlite.Conn {
	if a.pool == nil {
		if a.waitForPoolLimit == 0 {
			panic("Adapter not connected!")
		}
		start := time.Now()
		for {
			if a.pool != nil {
				break
			}
			if time.Since(start) >= a.waitForPoolLimit {
				return nil
			}
			time.Sleep(time.Microsecond)
		}
	}
	a.connInUse = a.connInUse + 1
	return a.pool.Get(nil)
}

// updatePairs generate the update pairs (one list of columns and one of values) used for update statement.
func (a *Adapter) updatePairs(param moleculer.Payload) ([]string, []interface{}) {
	columns := []string{}
	values := []interface{}{}
	param.ForEach(func(key interface{}, value moleculer.Payload) bool {
		col, ok := key.(string)
		if !ok {
			a.log.Error("extractFields() key must be string! - key: ", key)
			return false
		}
		columns = append(columns, a.ColName(col)+" = ?")
		values = append(values, value.Value())
		return true
	})
	return columns, values
}

// insertFields will parse the payload and extract the column names with
// value placeholders for the INSERT stmt.
// It will also return the values.
func (a *Adapter) insertFields(param moleculer.Payload) ([]string, []interface{}) {
	columns := []string{}
	values := []interface{}{}
	param.ForEach(func(key interface{}, value moleculer.Payload) bool {
		col, ok := key.(string)
		if !ok {
			a.log.Error("extractFields() key must be string! - key: ", key)
			return false
		}
		v := a.transformIn(col, value.Value())
		if v != nil {
			columns = append(columns, a.ColName(col))
			values = append(values, v)
		}
		return true
	})
	return columns, values
}

func (a *Adapter) populateStmt(stmt *sqlite.Stmt, param moleculer.Payload, fields []string) (err error) {
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

func (a *Adapter) loadSettings(settings map[string]interface{}) {
	if idField, ok := settings["idField"].(string); ok {
		a.idField = idField
	} else {
		a.idField = "id"
	}

	if fields, ok := settings["fields"].([]string); ok {
		a.fields = fields
	} else {
		a.fields = []string{"**"}
	}

	if uri, ok := settings["uri"].(string); ok {
		a.URI = uri
	}
}

// columnsDefinition return the column definitions for CREATE TABLE
func (a *Adapter) columnsDefinition() []string {
	columns := []string{a.idField + " INTEGER PRIMARY KEY AUTOINCREMENT"}
	for _, c := range a.Columns {
		def := c.Name
		if c.Type != "" {
			def = def + " " + dbType(c.Type)
		}
		columns = append(columns, def)
	}
	return columns
}

func (a *Adapter) createTable() error {
	resChan := make(chan moleculer.Payload, 1)
	go func() {
		defer a.catchConnError("Error on create table", resChan)
		conn := a.getConn()
		if conn == nil {
			resChan <- noConnectionError()
		}
		defer a.returnConn(conn)

		create := "CREATE TABLE IF NOT EXISTS " + a.Table + " (" + strings.Join(a.columnsDefinition(), ", ") + ");"
		a.log.Debug(create)

		err := sqlitex.ExecTransient(conn, create, nil)
		if err != nil {
			resChan <- payload.New(err)
		}
		a.log.Debug("table " + a.Table + " created !!!")
		resChan <- payload.Empty()
	}()
	p := <-resChan
	if p.IsError() {
		return p.Error()
	}
	return nil
}

func (a *Adapter) Find(param moleculer.Payload) moleculer.Payload {
	resChan := make(chan moleculer.Payload, 1)
	go func() {
		defer a.catchConnError("Error on find", resChan)
		conn := a.getConn()
		if conn == nil {
			resChan <- noConnectionError()
			return
		}
		defer a.returnConn(conn)
		resChan <- a.query(conn, a.findFields(param), param, a.rowToPayload)
	}()
	return <-resChan
}

func (a *Adapter) FindAndUpdate(param moleculer.Payload) moleculer.Payload {
	results := make(chan moleculer.Payload, 1)
	go func() {
		defer a.catchConnError("Error on find and update", results)
		conn := a.getConn()
		if conn == nil {
			results <- noConnectionError()
			return
		}
		defer a.returnConn(conn)
		update := param.Get("update")
		param = param.Remove("update")

		originals := a.query(conn, a.findFields(param), param, a.rowToPayload)
		if originals.IsError() {
			results <- originals
			return
		}
		result := []moleculer.Payload{}
		for _, item := range originals.Array() {
			id := item.Get(a.idField)
			if err := a.updateById(conn, id, update); err != nil {
				result = append(result, payload.New(err))
			} else {
				filter := payload.New(map[string]interface{}{
					"query": map[string]interface{}{a.idField: id.Value()},
				})
				updated := a.query(conn, a.findFields(filter), filter, a.rowToPayload).First()
				result = append(result, updated)
			}
		}
		results <- payload.New(result)
	}()
	return <-results
}

func (a *Adapter) Update(params moleculer.Payload) moleculer.Payload {
	id := params.Get("id")
	if !id.Exists() {
		return payload.Error("Cannot update record without id")
	}
	return a.UpdateById(id, params.Remove("id"))
}

func (a *Adapter) UpdateById(id, update moleculer.Payload) moleculer.Payload {
	results := make(chan moleculer.Payload, 1)
	go func() {
		defer a.catchConnError("Error on update by id: "+id.String(), results)
		conn := a.getConn()
		if conn == nil {
			results <- noConnectionError()
			return
		}
		defer a.returnConn(conn)
		if err := a.updateById(conn, id, update); err != nil {
			results <- payload.New(err)
			return
		}
		results <- a.findById(conn, id)
	}()
	return <-results
}

func (a *Adapter) Insert(param moleculer.Payload) moleculer.Payload {
	resChan := make(chan moleculer.Payload, 1)
	go func() {
		defer a.catchConnError("Error on insert", resChan)
		conn := a.getConn()
		if conn == nil {
			resChan <- noConnectionError()
			return
		}
		defer a.returnConn(conn)

		columns, values := a.insertFields(param)
		insert := "INSERT INTO " + a.Table + " (" + strings.Join(columns, ", ") + ") VALUES(" + strings.Join(placeholders(columns), ", ") + ") ;"
		a.log.Debug(insert)
		a.log.Debug("values: ", values)
		if err := sqlitex.Exec(conn, insert, nil, values...); err != nil {
			a.log.Error("Error on insert: ", err, " - values: ", values)
			resChan <- payload.New(err)
			return
		}
		resChan <- param.Add(a.idField, conn.LastInsertRowID())
	}()
	return <-resChan
}

func (a *Adapter) RemoveAll() moleculer.Payload {
	resChan := make(chan moleculer.Payload, 1)
	go func() {
		defer a.catchConnError("Error on remove all", resChan)
		conn := a.getConn()
		if conn == nil {
			resChan <- noConnectionError()
			return
		}
		defer a.returnConn(conn)

		delete := "DELETE FROM " + a.Table + " ;"
		a.log.Debug(delete)
		if err := sqlitex.Exec(conn, delete, nil); err != nil {
			a.log.Error("Error on delete: ", err)
			resChan <- payload.New(err)
			return
		}
		deletedCount := conn.Changes()
		resChan <- payload.New(map[string]int{"deletedCount": deletedCount})
	}()
	return <-resChan
}

func (a *Adapter) RemoveById(id moleculer.Payload) moleculer.Payload {
	resChan := make(chan moleculer.Payload, 1)
	go func() {
		defer a.catchConnError("Error on remove by id: "+id.String(), resChan)
		conn := a.getConn()
		if conn == nil {
			resChan <- noConnectionError()
			return
		}
		defer a.returnConn(conn)

		delete := "DELETE FROM " + a.Table + " WHERE id = " + id.String() + " ;"
		a.log.Debug(delete)
		if err := sqlitex.Exec(conn, delete, nil); err != nil {
			a.log.Error("Error on delete: ", err)
			resChan <- payload.New(err)
			return
		}
		deletedCount := conn.Changes()
		resChan <- payload.New(map[string]int{"deletedCount": deletedCount})
	}()
	return <-resChan
}

func (a *Adapter) FindById(id moleculer.Payload) moleculer.Payload {
	resChan := make(chan moleculer.Payload, 1)
	go func() {
		defer a.catchConnError("Error on find by id: "+id.String(), resChan)
		conn := a.getConn()
		if conn == nil {
			resChan <- noConnectionError()
			return
		}
		defer a.returnConn(conn)
		resChan <- a.findById(conn, id)
	}()
	return <-resChan
}

// FindByIds
func (a *Adapter) FindByIds(ids moleculer.Payload) moleculer.Payload {
	resChan := make(chan moleculer.Payload, 1)
	go func() {
		defer a.catchConnError("Error on find by ids: "+ids.String(), resChan)
		conn := a.getConn()
		if conn == nil {
			resChan <- noConnectionError()
			return
		}
		defer a.returnConn(conn)
		if !ids.IsArray() {
			resChan <- payload.Error("FindByIds() only support lists!")
			return
		}
		list := make([]moleculer.Payload, ids.Len())
		ids.ForEach(func(idx interface{}, id moleculer.Payload) bool {
			list[idx.(int)] = a.findById(conn, id)
			return true
		})
		resChan <- payload.New(list)
	}()
	return <-resChan
}

func (a *Adapter) FindOne(params moleculer.Payload) moleculer.Payload {
	return a.Find(params.Add("limit", 1)).First()
}

func (a *Adapter) Count(param moleculer.Payload) moleculer.Payload {
	resChan := make(chan moleculer.Payload, 1)
	go func() {
		defer a.catchConnError("Error on count ", resChan)
		conn := a.getConn()
		if conn == nil {
			resChan <- noConnectionError()
			return
		}
		defer a.returnConn(conn)
		resChan <- a.query(conn, []string{"COUNT(*) as count"}, param, func(fields []string, stmt *sqlite.Stmt) moleculer.Payload {
			return payload.New(stmt.GetInt64("count"))
		}).First()
	}()
	return <-resChan
}

func (a *Adapter) updateById(conn *sqlite.Conn, id, update moleculer.Payload) error {
	changes, values := a.updatePairs(update)
	updtStmt := "UPDATE " + a.Table + " SET " + strings.Join(changes, ", ") + " WHERE id=" + id.String() + ";"
	a.log.Debug(updtStmt, " - values: ", values)
	if err := sqlitex.Exec(conn, updtStmt, nil, values...); err != nil {
		a.log.Error("Error on update: ", err)
		return err
	}
	a.log.Debug("update done.")
	return nil
}

func (a *Adapter) findById(conn *sqlite.Conn, id moleculer.Payload) moleculer.Payload {
	filter := payload.New(map[string]interface{}{
		"query": map[string]interface{}{a.idField: id.Value()},
		"limit": 1,
	})
	return a.query(conn, a.findFields(filter), filter, a.rowToPayload).First()
}

func resolveFindOptions(param moleculer.Payload) (limit, offset string, sort []string) {
	if param.Get("limit").Exists() {
		limit = param.Get("limit").String()
	}
	if param.Get("offset").Exists() {
		offset = param.Get("offset").String()
	}
	if param.Get("sort").Exists() {
		if param.Get("sort").IsArray() {
			sort = sortsFromStringArray(param.Get("sort"))
		} else {
			sort = sortsFromString(param.Get("sort"))
		}
	}
	return limit, offset, sort
}

func sortsFromString(sort moleculer.Payload) []string {
	parts := strings.Split(strings.Trim(sort.String(), " "), " ")
	if len(parts) > 1 {
		sorts := []string{}
		for _, value := range parts {
			sorts = append(sorts, sortEntry(value))
		}
		return sorts
	} else if len(parts) == 1 && parts[0] != "" {
		return []string{sortEntry(parts[0])}
	}
	fmt.Println("**** invalid Sort Entry **** ")
	return []string{}
}

func sortsFromStringArray(sort moleculer.Payload) []string {
	sorts := []string{}
	sort.ForEach(func(index interface{}, value moleculer.Payload) bool {
		sorts = append(sorts, sortEntry(value.String()))
		return true
	})
	return sorts
}

func sortEntry(entry string) string {
	if strings.Index(entry, "-") == 0 {
		entry = strings.Replace(entry, "-", "", 1) + " DESC"
	} else {
		entry = strings.Replace(entry, "-", "", 1) + " ASC"
	}
	return entry
}

//findFields take the default fields from service settings.
// check if there are fields as parameters.
// check if all fields should be included **
// remove invalid field names
// always returs at least one field, idField
func (a *Adapter) findFields(param moleculer.Payload) []string {
	fields := a.fields
	if param.Get("fields").Exists() && param.Get("fields").IsArray() {
		fields = param.Get("fields").StringArray()
	}
	if len(fields) == 1 && fields[0] == "**" {
		fields = []string{}
		for _, c := range a.Columns {
			fields = append(fields, c.Name)
		}
	}
	fields = append(a.cleanFields(fields), a.idField)
	return fields
}

func (a *Adapter) validField(field string) bool {
	return field != "**" && field != "" && (hasColumn(field, a.Columns) || field == a.idField)
}

func hasColumn(name string, cols []Column) bool {
	for _, col := range cols {
		if col.Name == name {
			return true
		}
	}
	return false
}

func findColumn(name string, cols []Column) *Column {
	for _, col := range cols {
		if col.Name == name {
			return &col
		}
	}
	return nil
}

func (a *Adapter) cleanFields(fields []string) []string {
	list := []string{}
	for _, f := range fields {
		if a.validField(f) {
			list = append(list, f)
		}
	}
	return list
}

type rowFactory func([]string, *sqlite.Stmt) moleculer.Payload

func (a *Adapter) query(conn *sqlite.Conn, fields []string, param moleculer.Payload, mapRow rowFactory) moleculer.Payload {
	limit, offset, sort := resolveFindOptions(param)

	rows := []moleculer.Payload{}
	where := a.findWhere(param)
	selec := "SELECT " + strings.Join(fields, ", ") + " FROM " + a.Table
	if where != "" {
		selec = selec + " WHERE " + where
	}
	if len(sort) > 0 {
		selec = selec + " ORDER BY " + strings.Join(sort, ", ")
	}
	if limit != "" {
		selec = selec + " LIMIT " + limit
	}
	if offset != "" {
		selec = selec + " OFFSET " + offset
	}
	selec = selec + " ;"

	a.log.Trace(selec)
	if err := sqlitex.Exec(conn, selec, func(stmt *sqlite.Stmt) error {
		rows = append(rows, mapRow(fields, stmt))
		return nil
	}); err != nil {
		a.log.Error("Error on select: ", err)
		return payload.New(err)
	}
	a.log.Trace("rows: ", rows)
	return payload.New(rows)
}

func (a *Adapter) columnValue(column string, stmt *sqlite.Stmt) interface{} {
	t := a.columnType(column)
	if t == "NUMBER" {
		return stmt.GetFloat(column)
	}
	if t == "INTEGER" {
		return stmt.GetInt64(column)
	}
	return stmt.GetText(column)
}

var listSeparator = "||"

// transformIn transform a value to be send to the database (IN)
// receives the field name and the value.
// return the values that should be inserted in the database.
func (a *Adapter) transformIn(field string, value interface{}) interface{} {
	if value == nil {
		return nil
	}
	c := findColumn(field, a.Columns)
	if c == nil {
		return nil
	}
	t := c.Type
	if t == "[]string" {
		list, valid := value.([]string)
		if !valid {
			return nil
		}
		return strings.Join(list, listSeparator)
	}
	if t == "map" {
		m, valid := value.(map[string]interface{})
		if !valid {
			return nil
		}
		return a.serializer.MapToString(m)
	}
	if t == "[]byte" {
		bytes, valid := value.([]byte)
		if !valid {
			return nil
		}
		return string(bytes)
	}
	if t == "[]int" {
		list, valid := value.([]int)
		if !valid {
			return nil
		}
		sList := []string{}
		for _, v := range list {
			sList = append(sList, string(v))
		}
		return strings.Join(sList, listSeparator)
	}
	if t == "date" || t == "datetime" {
		t, valid := value.(time.Time)
		if !valid {
			sValue, valid := value.(string)
			if valid {
				_, sError := time.Parse(ISO8601, sValue)
				if sError != nil {
					return sValue
				}
			}
			return nil
		}
		v := t.UTC().Format(ISO8601)
		return v
	}
	return value
}

// transformOut transform a values returned from the database (OUT)
func (a *Adapter) transformOut(field string, value interface{}) interface{} {
	if value == nil || value == "" {
		return nil
	}
	c := findColumn(field, a.Columns)
	if c == nil {
		if field != a.idField {
			return nil
		}
		if a.idColumn == nil {
			a.idColumn = &Column{a.idField, "string"}
		}
		c = a.idColumn
	}
	t := c.Type
	if t == "bool" {
		return value.(string) == "1"
	}
	if t == "[]string" {
		return strings.Split(value.(string), listSeparator)
	}
	if t == "map" {
		return a.serializer.StringToMap(value.(string))
	}
	if t == "[]byte" {
		return []byte(value.(string))
	}
	if t == "[]int" {
		list := []int{}
		for _, s := range strings.Split(value.(string), listSeparator) {
			i, err := strconv.Atoi(s)
			if err != nil {
				list = append(list, i)
			}
		}
		return list
	}
	return value
}

func (a *Adapter) rowToPayload(fields []string, stmt *sqlite.Stmt) moleculer.Payload {
	data := map[string]interface{}{}
	for _, c := range fields {
		value := a.transformOut(c, a.columnValue(c, stmt))
		if value == 0 || value == "" {
			break
		}
		data[c] = value
	}
	return payload.New(data)
}

func dbType(t string) string {
	if t == "string" {
		return "TEXT"
	}
	if t == "date" || t == "datetime" {
		return "TEXT"
	}
	if t == "[]string" {
		return "TEXT"
	}
	if t == "[]integer" {
		return "TEXT"
	}
	if t == "[]byte" {
		return "TEXT"
	}
	if t == "float" {
		return "REAL"
	}
	return strings.ToUpper(t)
}

func (a *Adapter) columnType(field string) (r string) {
	for _, c := range a.Columns {
		if c.Name == field {
			return dbType(c.Type)
		}
	}
	return r
}

//betweenValues prepare the values for the operator "between" sql stmt -> between  A and B
func (a *Adapter) betweenValues(field string, values moleculer.Payload) (r string) {
	pair := values.Array()
	return a.wrapValue(field, pair[0]) + " AND " + a.wrapValue(field, pair[1])
}

//inValues prepare the values for the operator "in" sql stmt -> in (A, B, C)
func (a *Adapter) inValues(field string, values moleculer.Payload) (r string) {
	items := []string{}
	values.ForEach(func(key interface{}, item moleculer.Payload) bool {
		items = append(items, a.wrapValue(field, item))
		return true
	})
	return "(" + strings.Join(items, ",") + ")"
}

func (a *Adapter) orValues(values moleculer.Payload) (r string) {
	pairs := []string{}
	values.ForEach(func(idx interface{}, query moleculer.Payload) bool {
		for _, pair := range a.filterPairs(query) {
			pairs = append(pairs, pair)
		}
		return true
	})
	return strings.Join(pairs, " OR ")
}

//expressionValue when the filter clause is an expression, this function will
//return the operator and the values formated for SQLStmts
func (a *Adapter) expressionValue(field string, expression moleculer.Payload) (rField, value, operation string) {
	rField = field
	if strings.ToLower(field) == "or" {
		rField = ""
		value = a.orValues(expression)
	} else {
		expression.ForEach(func(key interface{}, item moleculer.Payload) bool {
			operation = strings.ToLower(key.(string))
			if operation == "between" || operation == "not between" {
				value = a.betweenValues(field, item)
			} else if operation == "in" || operation == "not in" {
				value = a.inValues(field, item)
			} else {
				value = a.wrapValue(field, item)
			}
			return false
		})
	}
	return rField, value, operation
}

//valueAndOperator return the value properly formated for SQL Stmt and the operator to be used in the where clause.
func (a *Adapter) valueAndOperator(field string, expression moleculer.Payload) (pair string) {
	operation := "="
	value := ""
	if expression.IsMap() || expression.IsArray() {
		field, value, operation = a.expressionValue(field, expression)
	} else {
		value = a.wrapValue(field, expression)
	}
	if a.isExpression(expression.String()) {
		operation = " "
	}
	return strings.TrimSpace(field + " " + operation + " " + value)
}

func (a *Adapter) isExpression(value string) bool {
	value = strings.ToUpper(value)
	return value == "IS NOT NULL" || value == "IS NULL"
}

func (a *Adapter) wrapValue(field string, value moleculer.Payload) (r string) {
	if a.isExpression(value.String()) {
		return strings.ToUpper(value.String())
	}

	cType := a.columnType(field)
	if cType == "TEXT" && isTime(value) {
		t, _ := value.Value().(time.Time)
		return "'" + t.UTC().Format(ISO8601) + "'"
	}
	if cType == "TEXT" || cType == "" {
		return "'" + value.String() + "'"
	}
	if cType == "NUMBER" {
		return "'" + strconv.FormatFloat(value.Float(), 'f', 6, 64) + "'"
	}
	if cType == "INTEGER" {
		return fmt.Sprint(value.Int64())
	}
	return r
}

func isTime(p moleculer.Payload) bool {
	_, valid := p.Value().(time.Time)
	return valid
}

//filterPairs create the where clause filter pairs: example. userName = 'John'
//uses a mongo-esq style for advanced filters, examples:
// "query": M{
// 	"age": M{
// 		">": 60,
// 	},
// },
//will result in:
// where age > 60
func (a *Adapter) filterPairs(query moleculer.Payload) (pairs []string) {
	query.ForEach(func(key interface{}, item moleculer.Payload) bool {
		field := key.(string)
		pair := a.valueAndOperator(field, item)
		pairs = append(pairs, pair)
		return true
	})
	return pairs
}

func (a *Adapter) updateWhere(params moleculer.Payload) string {
	where := ""
	queryPairs := a.filterPairs(params)
	if len(queryPairs) > 0 {
		where = strings.Join(queryPairs, " AND ")
	}
	return where
}

func (a *Adapter) findWhere(params moleculer.Payload) string {
	query := payload.Empty()
	if params.Get("query").Exists() {
		query = params.Get("query")
	}
	where := ""
	queryPairs := a.filterPairs(query)
	if len(queryPairs) > 0 {
		where = strings.Join(queryPairs, " AND ")
	}
	searchPairs := a.parseSearchFields(params)
	if len(searchPairs) > 0 {
		if where != "" {
			where = where + " AND "
		}
		where = where + "(" + strings.Join(searchPairs, " OR ") + ")"
	}
	return where
}

func (a *Adapter) parseSearchFields(params moleculer.Payload) (pairs []string) {
	searchFields := params.Get("searchFields")
	search := params.Get("search")
	searchValue := ""
	if search.Exists() {
		searchValue = search.String()
	}
	if searchFields.Exists() {
		if searchFields.IsArray() {
			fields := searchFields.StringArray()
			for _, field := range fields {
				pairs = append(pairs, field+" = "+"'"+searchValue+"'")
			}
		} else {
			pairs = []string{searchFields.String() + " = " + "'" + searchValue + "'"}
		}
	}
	return pairs
}
