package namespace

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestNamespace(t *testing.T) {
	t.Parallel()
	for i, b := range []any{new(raftClusterOpts), new(fileOpts), nil} {
		t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
			client, closer := testVaultServer(t, b)
			defer closer()

			ctx := context.Background()
			logical := client.Logical()

			rootNS := ""
			for _, ns := range []string{"pname", "cname", "dname", "ename"} {
				client.SetNamespace(rootNS)
				_, err := logical.WriteWithContext(ctx, "sys/namespaces/"+ns, nil)
				if err != nil {
					t.Fatal(err)
				}
				rspn, err := logical.ListWithContext(ctx, "sys/namespaces")
				if err != nil {
					t.Fatal(err)
				}
				if rspn.Data == nil ||
					rspn.Data["keys"] == nil ||
					!slices.Contains(rspn.Data["keys"].([]any), any(ns+"/")) {
					t.Errorf("Namespace list of %s: %+v", ns, rspn.Data)
				}
				rootNS += "/" + ns
			}

			client.SetNamespace("pname/cname")
			_, err := logical.DeleteWithContext(ctx, "sys/namespaces/dname")
			if err == nil || !strings.HasSuffix(err.Error(), "containing child namespaces") {
				t.Fatalf("Delete dname when ename exists: %s", err)
			}

			for _, ns := range []string{"ename", "dname", "cname", "pname"} {
				rootNS = strings.TrimSuffix(rootNS, "/"+ns)
				client.SetNamespace(rootNS)
				_, err := logical.DeleteWithContext(ctx, "sys/namespaces/"+ns)
				if err != nil {
					t.Fatal(err)
				}
				time.Sleep(time.Second)
				rspn, err := logical.ListWithContext(ctx, "sys/namespaces")
				if err != nil {
					t.Fatal(err)
				}
				if rootNS != "" && rspn != nil { // nil is correct response for zero sub-namespace
					t.Errorf("after delete %s, Namespace list of %s => %+v", ns, rootNS, rspn)
				}
			}
		})
	}
}
