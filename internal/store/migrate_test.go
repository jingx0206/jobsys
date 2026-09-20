package store

import (
	"context"
	"strings"
	"testing"
	"testing/fstest"
)

func TestMigrateAppliesEachFileOnce(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	t.Cleanup(func() {
		db.Pool.Exec(context.Background(), `DROP TABLE IF EXISTS migrate_test_a, migrate_test_bad`)
		db.Pool.Exec(context.Background(), `DELETE FROM schema_migrations WHERE version LIKE 'zz_test_%'`)
	})
	files := fstest.MapFS{
		"zz_test_1.sql": {Data: []byte("CREATE TABLE migrate_test_a (n int);\nINSERT INTO migrate_test_a VALUES (1);")},
		"zz_test_2.sql": {Data: []byte("INSERT INTO migrate_test_a VALUES (2);")},
	}

	applied, err := db.Migrate(ctx, files)
	if err != nil {
		t.Fatalf("first migrate: %v", err)
	}
	if strings.Join(applied, ",") != "zz_test_1.sql,zz_test_2.sql" {
		t.Errorf("first migrate applied %v, want both files in order", applied)
	}

	applied, err = db.Migrate(ctx, files)
	if err != nil || len(applied) != 0 {
		t.Errorf("second migrate applied %v (err %v), want nothing", applied, err)
	}
	var rows int
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM migrate_test_a`).Scan(&rows); err != nil || rows != 2 {
		t.Errorf("migrate_test_a has %d rows (err %v), want 2: each file ran once", rows, err)
	}
}

func TestMigrateRollsBackAFailingFile(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	t.Cleanup(func() {
		db.Pool.Exec(context.Background(), `DROP TABLE IF EXISTS migrate_test_bad`)
		db.Pool.Exec(context.Background(), `DELETE FROM schema_migrations WHERE version LIKE 'zz_test_%'`)
	})
	files := fstest.MapFS{
		"zz_test_bad.sql": {Data: []byte("CREATE TABLE migrate_test_bad (n int);\nSELEC broken;")},
	}

	if _, err := db.Migrate(ctx, files); err == nil || !strings.Contains(err.Error(), "zz_test_bad.sql") {
		t.Fatalf("err = %v, want an error naming the failing file", err)
	}

	var tableExists, recorded bool
	db.Pool.QueryRow(ctx, `SELECT to_regclass('migrate_test_bad') IS NOT NULL`).Scan(&tableExists)
	db.Pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = 'zz_test_bad.sql')`).Scan(&recorded)
	if tableExists || recorded {
		t.Errorf("after a failed file: table created=%v, recorded=%v; want neither", tableExists, recorded)
	}
}
