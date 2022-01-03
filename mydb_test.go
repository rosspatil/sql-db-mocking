package mydb

import (
	"strconv"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

func TestDB_Ping(t *testing.T) {
	masterDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	replica1, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	replica2, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	db, err := New(masterDB, replica1, replica2)
	assert.Nil(t, err)

	// Success test for ping
	err = db.Ping()
	assert.Nil(t, err)

	// master db is closed
	masterDB.Close()
	err = db.Ping()
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), "master's db ping fail: sql: database is closed")

	// replica1 db and master db are closed
	replica1.Close()
	err = db.Ping()
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), "master's db ping fail: sql: database is closed\nreplica db 1 ping fail: sql: database is closed")
}

func TestDB_Query(t *testing.T) {
	masterDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	replica1, mock1, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	replica2, mock2, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	db, err := New(masterDB, replica1, replica2)
	assert.Nil(t, err)
	rows1 := sqlmock.NewRows([]string{"col1, col2"})
	rows2 := sqlmock.NewRows([]string{"col3, col4"})
	mock1.ExpectQuery("Query1").WillReturnRows(rows1)
	mock2.ExpectQuery("Query1").WillReturnRows(rows2)
	// Success case
	rs, err := db.Query("Query1")
	assert.Nil(t, err)
	cols, err := rs.Columns()
	assert.Nil(t, err)
	assert.Equal(t, cols, []string{"col3, col4"})

	rs, err = db.Query("Query1")
	assert.Nil(t, err)
	cols, err = rs.Columns()
	assert.Nil(t, err)
	assert.Equal(t, cols, []string{"col1, col2"})

	// replica1 is closed or under maintanace
	// This test always return ["col3, col4"] from replica 2
	replica1.Close()
	for index := 0; index < 5; index++ {
		i := strconv.Itoa(index + 2)
		mock2.ExpectQuery("Query" + i).WillReturnRows(rows2)
		rs, err := db.Query("Query" + i)
		assert.Nil(t, err)
		cols, err := rs.Columns()
		assert.Nil(t, err)
		assert.Equal(t, cols, []string{"col3, col4"})
	}

	// replica 2 is also closed
	// Now no replica is available to read so this test gives error
	replica2.Close()
	rs, err = db.Query("Query1")
	assert.Nil(t, rs)
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), noReplicaAvailableError)
}

func TestDB_QueryRow(t *testing.T) {
	masterDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	replica1, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	db, err := New(masterDB, replica1)
	assert.Nil(t, err)
	rows := db.QueryRow("Query1")
	assert.NotNil(t, rows)
}

func TestDB_Exec(t *testing.T) {
	masterDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	replica1, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	db, err := New(masterDB, replica1)
	assert.Nil(t, err)

	// Succes case to insert data and return id 123, only 1 row is RowsAffected
	mock.ExpectExec("Query1").WillReturnResult(sqlmock.NewResult(123, 1))
	rs, err := db.Exec("Query1")
	assert.Nil(t, err)
	id, err := rs.LastInsertId()
	assert.Nil(t, err)

	assert.Equal(t, id, int64(123))
	rowsAffected, err := rs.RowsAffected()
	assert.Nil(t, err)
	assert.Equal(t, rowsAffected, int64(1))

	// master db is closed , which gives error
	masterDB.Close()
	mock.ExpectExec("Query2").WillReturnResult(sqlmock.NewResult(123, 1))
	rs, err = db.Exec("Query2")
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), "sql: database is closed")
	assert.Empty(t, rs)
}

func TestDB_Prepare(t *testing.T) {
	masterDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	replica1, mock1, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	replica2, mock2, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	db, err := New(masterDB, replica1, replica2)
	assert.Nil(t, err)
	rows1 := sqlmock.NewRows([]string{"col1, col2"})
	rows2 := sqlmock.NewRows([]string{"col3, col4"})

	mock1.ExpectPrepare("Select1").ExpectQuery().WillReturnRows(rows1)
	mock2.ExpectPrepare("Select1").ExpectQuery().WillReturnRows(rows2)

	stmt, err := db.Prepare("Select1")
	assert.Nil(t, err)
	rs, err := stmt.Query()
	assert.Nil(t, err)
	cols, err := rs.Columns()
	assert.Nil(t, err)
	assert.Equal(t, cols, []string{"col3, col4"})

	stmt, err = db.Prepare("Select1")
	assert.Nil(t, err)
	rs, err = stmt.Query()
	assert.Nil(t, err)
	cols, err = rs.Columns()
	assert.Nil(t, err)
	assert.Equal(t, cols, []string{"col1, col2"})

	replica2.Close()
	mock1.ExpectPrepare("Select2").ExpectQuery().WillReturnRows(rows1)
	stmt, err = db.Prepare("Select2")
	assert.Nil(t, err)
	rs, err = stmt.Query()
	assert.Nil(t, err)
	cols, err = rs.Columns()
	assert.Nil(t, err)
	assert.Equal(t, cols, []string{"col1, col2"})

	mock.ExpectPrepare("Insert").ExpectExec().WillReturnResult(sqlmock.NewResult(123, 1))
	stmt, err = db.Prepare("Insert")
	assert.Nil(t, err)
	result, err := stmt.Exec()
	assert.Nil(t, err)
	id, err := result.LastInsertId()
	assert.Nil(t, err)

	assert.Equal(t, id, int64(123))
	rowsAffected, err := result.RowsAffected()
	assert.Nil(t, err)
	assert.Equal(t, rowsAffected, int64(1))
}

func TestDB_New(t *testing.T) {
	masterDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	replica1, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}

	_, err = New(masterDB, replica1)
	assert.Nil(t, err)
	_, err = New(masterDB)
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), noReadReplicaError)
}
func TestDB_Close(t *testing.T) {
	masterDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	replica1, mock1, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}

	db, err := New(masterDB, replica1)
	assert.Nil(t, err)

	mock.ExpectClose().WillReturnError(nil)
	mock1.ExpectClose().WillReturnError(nil)

	err = db.Close()
	assert.Nil(t, err)
}

func TestDB_SetConnMaxLifetime(t *testing.T) {
	masterDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	replica1, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}

	db, err := New(masterDB, replica1)
	assert.Nil(t, err)
	db.SetMaxIdleConns(100)
	db.SetConnMaxLifetime(time.Hour * 24)
	db.SetMaxOpenConns(30)
}

func TestHello(t *testing.T) {
	Hello()
}
