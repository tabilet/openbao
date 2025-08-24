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
		"database": "openbao",
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
	if b.stable[0] != "superbao" {
		t.Fatalf("Table name does not match")
	}
}

func TestTDEngine_Namespace(t *testing.T) {
	ns := &namespace.Namespace{
		Path:           "pname",
		CustomMetadata: map[string]string{},
	}

	if tbl := tablename(ns); tbl != "pname" {
		t.Fatalf("Namespace table name does not match")
	}

	ns.Path = "pname/cname"
	if tbl := tablename(ns); tbl != "pname_cname" {
		t.Fatalf("Namespace table name does not match")
	}

	ns.Path = "pname/cname/dname"
	if tbl := tablename(ns); tbl != "pname_cname_dname" {
		t.Fatalf("Namespace table name does not match")
	}

	ns.Path = "pname/cname/dname/ename"
	if tbl := tablename(ns); tbl != "pname_cname_dname_ename" {
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
	if tname != "openbao.pname_cname_dname_ename" {
		t.Errorf("Table name does not match %s", tname)
	}
}
