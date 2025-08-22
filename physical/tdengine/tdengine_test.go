package tdengine

import (
	"context"
	"testing"

	log "github.com/hashicorp/go-hclog"
	"github.com/openbao/openbao/helper/namespace"
	"github.com/openbao/openbao/sdk/v2/helper/logging"
	_ "github.com/taosdata/driver-go/v3/taosSql"
)

func backend() *TDEngineBackend {
	logger := logging.NewVaultLogger(log.Debug)
	bi, err := NewTDEngineBackend(map[string]string{
		"connection_url": "root:taosdata@tcp(vm0:6030)/testbao",
		"database":       "testbao",
	}, logger)
	if err != nil {
		panic(err)
	}
	return bi
}

func TestTDEngine_Backend(t *testing.T) {
	b := backend()
	tableExist, err := b.existingTable("root")
	if err != nil {
		t.Fatalf("Failed to check table existence: %v", err)
	}
	if !tableExist {
		t.Fatalf("Table does not exist")
	}
	if b.database != "openbao" {
		t.Fatalf("Database name does not match")
	}
	if b.sTables["stable"][0] != "superbao" {
		t.Fatalf("Table name does not match")
	}
}

func TestTDEngine_Namespace(t *testing.T) {
	ns := &namespace.Namespace{
		ID:             "root/pname",
		CustomMetadata: map[string]string{},
	}

	if tbl := tablename(ns); tbl != "root_pname" {
		t.Fatalf("Namespace table name does not match")
	}

	ns.ID = "root/pname/cname"
	if tbl := tablename(ns); tbl != "root_pname_cname" {
		t.Fatalf("Namespace table name does not match")
	}

	ns.ID = "root/pname/cname/dname"
	if tbl := tablename(ns); tbl != "root_pname_cname_dname" {
		t.Fatalf("Namespace table name does not match")
	}

	ns.ID = "root/pname/cname/dname/ename"
	if tbl := tablename(ns); tbl != "root_pname_cname_dname_ename" {
		t.Fatalf("Namespace table name does not match")
	}

	ctx := namespace.ContextWithNamespace(context.Background(), ns)
	ns2, err := namespace.FromContext(ctx)
	if err != nil {
		t.Fatalf("Failed to get namespace from context: %v", err)
	}
	if ns2.ID != ns.ID {
		t.Fatalf("Namespace ID does not match")
	}

	b := backend()
	tname, err := b.getTablename(ctx)
	if err != nil {
		t.Fatalf("Failed to get table name: %v", err)
	}
	if tname != "openbao.root_pname_cname_dname_ename" {
		t.Errorf("Table name does not match %s", tname)
	}
}

/*
func TestTDEngine_TablesCreateDrop(t *testing.T) {
	b := backend()
	ns := &namespace.Namespace{
		ID:             "root",
		CustomMetadata: map[string]string{},
	}

	ctx := namespace.ContextWithNamespace(context.Background(), ns)
	if err := b.CreateIfNotExists(ctx, "pname"); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	ns.ID = "root/pname"
	ctx = namespace.ContextWithNamespace(context.Background(), ns)
	if err := b.CreateIfNotExists(ctx, "cname"); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	ns.ID = "root/pname/cname"
	ctx = namespace.ContextWithNamespace(context.Background(), ns)
	if err := b.CreateIfNotExists(ctx, "dname"); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	ns.ID = "root/pname/cname/dname"
	ctx = namespace.ContextWithNamespace(context.Background(), ns)
	if err := b.CreateIfNotExists(ctx, "ename"); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	ns.ID = "root/pname/cname"
	ctx = namespace.ContextWithNamespace(context.Background(), ns)
	if err := b.DropIfExists(ctx, "ename"); err.Error() != "namespace not found ename" {
		t.Fatalf("Failed to drop table: %v", err)
	}
	if err := b.DropIfExists(ctx, "dname"); err.Error() != "children namespace found dname" {
		t.Fatalf("Failed to drop table: %v", err)
	}

	ns.ID = "root/pname/cname/dname"
	ctx = namespace.ContextWithNamespace(context.Background(), ns)
	if err := b.DropIfExists(ctx, "ename"); err != nil {
		t.Fatalf("Failed to drop table: %v", err)
	}

	ns.ID = "root/pname/cname"
	ctx = namespace.ContextWithNamespace(context.Background(), ns)
	if err := b.DropIfExists(ctx, "dname"); err != nil {
		t.Fatalf("Failed to drop table: %v", err)
	}

	ns.ID = "root/pname"
	ctx = namespace.ContextWithNamespace(context.Background(), ns)
	if err := b.DropIfExists(ctx, "cname"); err != nil {
		t.Fatalf("Failed to drop table: %v", err)
	}

	ns.ID = "root"
	ctx = namespace.ContextWithNamespace(context.Background(), ns)
	if err := b.DropIfExists(ctx, "pname"); err != nil {
		t.Fatalf("Failed to drop table: %v", err)
	}
}
*/
