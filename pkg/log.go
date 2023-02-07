package pkg

import (
	"database/sql"
	"encoding/json"
	"github.com/huandu/go-sqlbuilder"
	"github.com/jmoiron/sqlx"
	"time"
)

type LogEntryTypeType int

const (
	// LogEntryTypeInt is the type for integer values
	LogEntryTypeInt LogEntryTypeType = iota
	LogEntryTypeReal
	LogEntryTypeText
	LogEntryTypeBlob
	LogEntryTypeJSON
)

type RowType struct {
	Name string
	Type LogEntryTypeType
}

type LogWriter struct {
	db *sqlx.DB

	Types map[string]RowType
}

func NewLogWriter(db *sqlx.DB, types map[string]RowType) *LogWriter {
	return &LogWriter{
		db:    db,
		Types: types,
	}
}

func (l *LogWriter) Close() error {
	return l.db.Close()
}

func (l *LogWriter) Write(p []byte) (n int, err error) {
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
		var typeValue LogEntryTypeType

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

		q := sqlbuilder.NewInsertBuilder()
		q.InsertInto("log_entries_meta").
			Cols("log_entry_id", "type", "name", "int_value", "real_value", "text_value", "blob_value").
			Values(logEntryID, typeValue, k, intValue, realValue, textValue, blobValue)
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
		Define("name", "VARCHAR(255)", "NOT NULL").
		Define("int_value", "INTEGER").
		Define("real_value", "REAL").
		Define("text_value", "TEXT").
		Define("blob_value", "BLOB")
	if _, err := l.db.Exec(ctb.String()); err != nil {
		return err
	}

	err := l.createTypeEnumTable()
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
		Values("json", LogEntryTypeJSON)
	s, args := q.Build()
	if _, err := l.db.Exec(s, args...); err != nil {
		return err
	}

	return nil
}
