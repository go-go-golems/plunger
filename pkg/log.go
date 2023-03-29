package pkg

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/huandu/go-sqlbuilder"
	"github.com/jmoiron/sqlx"
	"time"
)

// LogEntryType represents the different types LogEntries can have.
type LogEntryType int

const (
	LogEntryTypeInt LogEntryType = iota
	LogEntryTypeReal
	LogEntryTypeText
	LogEntryTypeBlob
	LogEntryTypeJSON
)

type Row struct {
	Name string
	Type LogEntryType
}

// MetaKey is used to store keys that are often used, to avoid storing them as full strings.
// Instead, we store them by id and use a lookup table to get the full string.
type MetaKey struct {
	Name string
	ID   int
}

// MetaKeys is a collection of MetaKey. It is used to quickly manage
// adding new keys.
type MetaKeys struct {
	Keys      map[string]*MetaKey
	namesById map[int]string
	maxID     int
}

func NewMetaKeys() *MetaKeys {
	return &MetaKeys{
		Keys:      make(map[string]*MetaKey),
		namesById: make(map[int]string),
		maxID:     0,
	}
}

func (m *MetaKeys) Get(name string) (*MetaKey, bool) {
	key, ok := m.Keys[name]
	return key, ok
}

func (m *MetaKeys) GetByID(id int) (*MetaKey, bool) {
	name, ok := m.namesById[id]
	if !ok {
		return nil, false
	}
	return m.Get(name)
}

func (m *MetaKeys) Add(name string) *MetaKey {
	key, ok := m.Get(name)
	if ok {
		return key
	}

	key = &MetaKey{
		Name: name,
		ID:   m.maxID,
	}
	m.maxID++
	m.Keys[name] = key
	m.namesById[key.ID] = name
	return key
}

func (m *MetaKeys) AddWithID(name string, id int) (*MetaKey, error) {
	name_, ok := m.namesById[id]
	if ok {
		if name_ != name {
			return nil, fmt.Errorf("key %s already exists with id %d", name_, id)
		}
	}

	key, ok := m.Get(name)
	if ok {
		if key.ID != id {
			return key, fmt.Errorf("key %s already exists with id %d", name, key.ID)
		}
		return key, nil
	}

	key = &MetaKey{
		Name: name,
		ID:   id,
	}
	if id > m.maxID {
		m.maxID = id
	}
	m.maxID++
	m.namesById[id] = name
	m.Keys[name] = key

	return key, nil
}

// Schema is a set of MetaKeys
type Schema struct {
	MetaKeys *MetaKeys
}

func NewSchema() *Schema {
	return &Schema{
		MetaKeys: NewMetaKeys(),
	}
}

// LogWriter is the main class in Plunger.
//
// It deserializes the JSON binaries handed over by zerolog, and decomposes
// the message into the database schema specified at creation time.
type LogWriter struct {
	db *sqlx.DB

	schema *Schema
}

func NewLogWriter(db *sqlx.DB, schema *Schema) *LogWriter {
	return &LogWriter{
		db:     db,
		schema: schema,
	}
}

func (l *LogWriter) Close() error {
	if l.db != nil {
		return l.db.Close()
	} else {
		return nil
	}
}

func (l *LogWriter) Write(p []byte) (int, error) {
	var log map[string]interface{}
	if err := json.Unmarshal(p, &log); err != nil {
		return 0, err
	}

	tx, err := l.db.Beginx()
	if err != nil {
		return 0, err
	}
	defer func() {
		if err != nil {
			err = tx.Rollback()
			return
		}
		err = tx.Commit()
	}()

	// Insert the log entry
	logEntryID := 0
	q := sqlbuilder.NewInsertBuilder()
	q.InsertInto("log_entries").
		Cols("date", "level", "session").
		Values(time.Now().UTC(), log["level"], log["session"]).
		SQL("RETURNING id")
	s, args := q.Build()
	if err := tx.QueryRowx(s, args...).Scan(&logEntryID); err != nil {
		return 0, err
	}

	// Serialize the log data as log entries meta
	for k, v := range log {
		if k == "level" || k == "session" {
			continue
		}

		var intValue, realValue sql.NullInt64
		var textValue, blobValue sql.NullString
		var typeValue LogEntryType
		var name sql.NullString
		var meta_key_id sql.NullInt32

		switch v := v.(type) {
		case float64:
			realValue = sql.NullInt64{Int64: int64(v), Valid: true}
			typeValue = LogEntryTypeReal
		case int64:
			intValue = sql.NullInt64{Int64: v, Valid: true}
			typeValue = LogEntryTypeInt
		case string:
			textValue = sql.NullString{String: v, Valid: true}
			typeValue = LogEntryTypeText
		default:
			b, err := json.Marshal(v)
			if err != nil {
				return 0, err
			}
			blobValue = sql.NullString{String: string(b), Valid: true}
			typeValue = LogEntryTypeJSON
		}

		if metaKey, ok := l.schema.MetaKeys.Get(k); ok {
			meta_key_id = sql.NullInt32{Int32: int32(metaKey.ID), Valid: true}
		} else {
			name = sql.NullString{String: k, Valid: true}
		}

		q := sqlbuilder.NewInsertBuilder()
		q.InsertInto("log_entries_meta").
			Cols("log_entry_id", "type", "name", "meta_key_id", "int_value", "real_value", "text_value", "blob_value").
			Values(logEntryID, typeValue, name, meta_key_id, intValue, realValue, textValue, blobValue)
		s, args := q.Build()
		if _, err := tx.Exec(s, args...); err != nil {
			return 0, err
		}
	}

	return len(p), nil
}

func (l *LogWriter) Init() error {
	ctb := sqlbuilder.NewCreateTableBuilder()
	ctb.CreateTable("log_entries").
		IfNotExists().
		Define("id", "INTEGER", "PRIMARY KEY", "AUTOINCREMENT").
		Define("date", "TIMESTAMP", "NOT NULL").
		Define("level", "VARCHAR(255)", "NOT NULL").
		Define("session", "VARCHAR(255)")
	if _, err := l.db.Exec(ctb.String()); err != nil {
		return err
	}

	ctb = sqlbuilder.NewCreateTableBuilder()
	ctb.CreateTable("log_entries_meta").
		IfNotExists().
		Define("id", "INTEGER", "PRIMARY KEY", "AUTOINCREMENT").
		Define("log_entry_id", "INTEGER", "NOT NULL").
		Define("type", "VARCHAR(255)", "NOT NULL").
		Define("meta_key_id", "INTEGER").
		Define("name", "VARCHAR(255)").
		Define("int_value", "INTEGER").
		Define("real_value", "REAL").
		Define("text_value", "TEXT").
		Define("blob_value", "BLOB")

	if _, err := l.db.Exec(ctb.String()); err != nil {
		return err
	}

	// create indices using raw sql
	indexedColumns := []string{
		"log_entry_id",
		"type",
		"name",
	}
	for _, col := range indexedColumns {
		query := fmt.Sprintf("CREATE INDEX IF NOT EXISTS log_entries_meta_%s_idx ON log_entries_meta (%s)", col, col)
		_, err := l.db.Exec(query)
		if err != nil {
			return err
		}
	}

	err := l.createTypeEnumTable()
	if err != nil {
		return err
	}

	err = l.saveLogDBSchema()
	if err != nil {
		return err
	}

	return nil
}

// saveLogDBSchema stores the schema of the logwriter in the database.
//
// NOTE(manuel, 2023-02-06): This is a very naive implementation.
// It currently blindly overwrites it, but in the future, it will warn
// if there is a schema mismatch with what is already present.
func (l *LogWriter) saveLogDBSchema() error {
	err := l.saveMetaKeys()
	if err != nil {
		return err
	}

	return nil
}

func (l *LogWriter) createTypeEnumTable() error {
	ctb := sqlbuilder.NewCreateTableBuilder()
	ctb.CreateTable("type_enum").
		IfNotExists().
		Define("type", "VARCHAR(255)", "PRIMARY KEY").
		Define("seq", "INTEGER", "NOT NULL")
	if _, err := l.db.Exec(ctb.String()); err != nil {
		return err
	}

	// Insert the types using InsertBuilder
	q := sqlbuilder.NewInsertBuilder()
	q.InsertInto("type_enum").
		Cols("type", "seq").
		Values("int", LogEntryTypeInt).
		Values("real", LogEntryTypeReal).
		Values("text", LogEntryTypeText).
		Values("blob", LogEntryTypeBlob).
		Values("json", LogEntryTypeJSON).
		SQL("ON CONFLICT (type) DO NOTHING")
	s, args := q.Build()
	if _, err := l.db.Exec(s, args...); err != nil {
		return err
	}

	return nil
}

func (l *LogWriter) saveMetaKeys() error {
	ctb := sqlbuilder.NewCreateTableBuilder()
	ctb.CreateTable("meta_keys").
		IfNotExists().
		Define("id", "INTEGER", "PRIMARY KEY NOT NULL").
		Define("key", "VARCHAR(255)")
	if _, err := l.db.Exec(ctb.String()); err != nil {
		return err
	}

	// add unique index on key
	_, err := l.db.Exec("CREATE UNIQUE INDEX IF NOT EXISTS meta_keys_key_idx ON meta_keys (key)")
	if err != nil {
		return err
	}

	// Insert the keys using InsertBuilder
	if len(l.schema.MetaKeys.Keys) > 0 {
		q := sqlbuilder.NewInsertBuilder()
		q.InsertInto("meta_keys").
			Cols("id", "key")
		for _, v := range l.schema.MetaKeys.Keys {
			q.Values(v.ID, v.Name)
		}
		s, args := q.Build()
		if _, err := l.db.Exec(s, args...); err != nil {
			return err
		}
	}

	return nil
}
