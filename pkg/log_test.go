package pkg

import (
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"

	// sqlite
	_ "github.com/mattn/go-sqlite3"
)

func TestMetaKeys(t *testing.T) {
	k := NewMetaKeys()

	_, ok := k.Get("foo")
	assert.False(t, ok)

	v := k.Add("foo")
	assert.Equal(t, "foo", v.Name)
	assert.Equal(t, 0, v.ID)

	v, ok = k.GetByID(0)
	assert.True(t, ok)
	assert.Equal(t, "foo", v.Name)
	assert.Equal(t, 0, v.ID)

	_, ok = k.GetByID(1)
	assert.False(t, ok)

	v, ok = k.Get("foo")
	assert.True(t, ok)
	assert.Equal(t, "foo", v.Name)
	assert.Equal(t, 0, v.ID)

	v = k.Add("bar")
	assert.Equal(t, "bar", v.Name)
	assert.Equal(t, 1, v.ID)

	v, ok = k.GetByID(1)
	assert.True(t, ok)
	assert.Equal(t, "bar", v.Name)
	assert.Equal(t, 1, v.ID)

	v, ok = k.Get("bar")
	assert.True(t, ok)
	assert.Equal(t, "bar", v.Name)

	v, err := k.AddWithID("baz", 2)
	assert.NoError(t, err)
	assert.Equal(t, "baz", v.Name)
	assert.Equal(t, 2, v.ID)

	v, ok = k.Get("baz")
	assert.True(t, ok)
	assert.Equal(t, "baz", v.Name)
	assert.Equal(t, 2, v.ID)

	v, err = k.AddWithID("baz", 2)
	assert.NoError(t, err)
	assert.Equal(t, "baz", v.Name)
	assert.Equal(t, 2, v.ID)

	v, ok = k.Get("baz")
	assert.True(t, ok)
	assert.Equal(t, "baz", v.Name)
	assert.Equal(t, 2, v.ID)

	_, err = k.AddWithID("baz", 3)
	assert.Error(t, err)

	v, ok = k.Get("baz")
	assert.True(t, ok)
	assert.Equal(t, "baz", v.Name)
	assert.Equal(t, 2, v.ID)

	v, err = k.AddWithID("qux", 5)
	assert.NoError(t, err)
	assert.Equal(t, "qux", v.Name)
	assert.Equal(t, 5, v.ID)

	v, ok = k.GetByID(5)
	assert.True(t, ok)
	assert.Equal(t, "qux", v.Name)
	assert.Equal(t, 5, v.ID)

	v, ok = k.Get("qux")
	assert.True(t, ok)
	assert.Equal(t, "qux", v.Name)
	assert.Equal(t, 5, v.ID)

	v = k.Add("quz")
	assert.NoError(t, err)
	assert.Equal(t, "quz", v.Name)
	assert.Equal(t, 6, v.ID)

	v, ok = k.Get("quz")
	assert.True(t, ok)
	assert.Equal(t, "quz", v.Name)
	assert.Equal(t, 6, v.ID)
}

func TestLogWriterInit(t *testing.T) {
	db := sqlx.MustOpen("sqlite3", ":memory:")
	require.NotNil(t, db)
	defer db.Close()

	lw := NewLogWriter(db)
	assert.NotNil(t, lw)

	err := lw.Init()
	require.NoError(t, err)

	lw.schema.MetaKeys.Add("foo")
	lw.schema.MetaKeys.Add("bar")
	lw.schema.MetaKeys.Add("baz")

	err = lw.saveSchema()
	require.NoError(t, err)

	lw = NewLogWriter(db)
	err = lw.Init()
	require.NoError(t, err)

	v, ok := lw.schema.MetaKeys.Get("foo")
	require.True(t, ok)
	assert.Equal(t, "foo", v.Name)
	assert.Equal(t, 0, v.ID)

	v, ok = lw.schema.MetaKeys.Get("bar")
	require.True(t, ok)
	assert.Equal(t, "bar", v.Name)
	assert.Equal(t, 1, v.ID)

	v, ok = lw.schema.MetaKeys.Get("baz")
	require.True(t, ok)
	assert.Equal(t, "baz", v.Name)
	assert.Equal(t, 2, v.ID)

	lw.schema.MetaKeys.Add("qux")

	err = lw.saveSchema()
	require.NoError(t, err)

	lw = NewLogWriter(db)
	err = lw.Init()
	require.NoError(t, err)

	v, ok = lw.schema.MetaKeys.Get("qux")
	require.True(t, ok)
	assert.Equal(t, "qux", v.Name)
	assert.Equal(t, 3, v.ID)

	v, ok = lw.schema.MetaKeys.Get("foo")
	require.True(t, ok)
	assert.Equal(t, "foo", v.Name)
	assert.Equal(t, 0, v.ID)

}
