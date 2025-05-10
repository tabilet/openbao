package namespace

import (
	"context"
	"fmt"
	"testing"
)

func TestPolicyRootDefault(t *testing.T) {
	t.Parallel()
	for i, b := range []any{new(raftClusterOpts), new(fileOpts), nil} {
		t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
			client, closer := testVaultServer(t, b)
			defer closer()

			ctx := context.Background()

			logical := client.Logical()

			path := "approle"
			_, secretID, clientToken, err := getApprole(client, ctx, path, "myrole")
			if err != nil {
				t.Fatal(err)
			}

			rootToken := client.Token()
			client.SetToken(clientToken)

			secret, err := logical.ReadWithContext(ctx, "auth/token/lookup-self")
			if err != nil {
				t.Fatal(err)
			}

			if secret == nil || secret.Data == nil {
				t.Errorf("no secret")
			}
			if secret.Data["policies"].([]any)[0].(string) != "default" {
				t.Errorf("%#v", secret.Data)
			}

			_, err = logical.ReadWithContext(ctx, "auth/approle/role/myrole")
			if err == nil {
				t.Error("should be 403")
			}

			client.SetToken(rootToken)
			err = dropApprole(client, ctx, secretID, path, "myrole")
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPolicyRootCustom(t *testing.T) {
	t.Parallel()
	for i, b := range []any{new(raftClusterOpts), new(fileOpts), nil} {
		t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
			client, closer := testVaultServer(t, b)
			defer closer()

			ctx := context.Background()

			logical := client.Logical()

			name := "readpolicy"
			policies := []string{name}
			sys := client.Sys()
			err := sys.PutPolicyWithContext(ctx, name, getReadApproleRule())
			if err != nil {
				t.Fatal(err)
			}

			path := "approle"
			_, secretID, clientToken, err := getApprole(client, ctx, path, "myrole", policies...)
			if err != nil {
				t.Fatal(err)
			}

			rootToken := client.Token()
			client.SetToken(clientToken)
			secret, err := logical.ReadWithContext(ctx, "auth/token/lookup-self")
			if err != nil {
				t.Fatal(err)
			}
			if secret == nil || secret.Data == nil {
				t.Errorf("no secret")
			}
			if secret.Data["policies"].([]any)[0].(string) != "default" ||
				secret.Data["policies"].([]any)[1].(string) != name {
				t.Errorf("%#v", secret.Data)
			}

			secret, err = logical.ReadWithContext(ctx, "auth/approle/role/myrole")
			if err != nil {
				t.Fatal(err)
			}
			if secret == nil || secret.Data == nil {
				t.Errorf("%#v", secret.Data)
			}
			if secret.Data["policies"].([]any)[0].(string) != name {
				t.Errorf("%#v", secret.Data)
			}

			client.SetToken(rootToken)
			err = sys.DeletePolicyWithContext(ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			err = dropApprole(client, ctx, secretID, path, "myrole")
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPolicyNamespaceDefault(t *testing.T) {
	t.Parallel()
	for i, b := range []any{new(raftClusterOpts), new(fileOpts), nil} {
		t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
			client, closer := testVaultServer(t, b)
			defer closer()

			ctx := context.Background()

			rootNS := "pname"
			clone, err := cloneClient(ctx, client, rootNS)
			if err != nil {
				t.Fatal(err)
			}

			sys := clone.Sys()
			logical := clone.Logical()

			name := "default"
			err = sys.PutPolicyWithContext(ctx, name, getDefaultRule())
			if err != nil {
				t.Fatal(err)
			}

			path := "approle"
			_, secretID, clientToken, err := getApprole(clone, ctx, path, "myrole")
			if err != nil {
				t.Fatal(err)
			}

			rootToken := clone.Token()
			clone.SetToken(clientToken)

			secret, err := logical.ReadWithContext(ctx, "auth/token/lookup-self")
			if err != nil {
				t.Fatal(err)
			}
			if secret == nil || secret.Data == nil {
				t.Errorf("no secret")
			}
			if secret.Data["policies"].([]any)[0].(string) != name {
				t.Errorf("%#v", secret.Data)
			}

			_, err = logical.ReadWithContext(ctx, "auth/approle/role/myrole")
			if err == nil {
				t.Error("should be 403")
			}

			clone.SetToken(rootToken)
			err = sys.DeletePolicyWithContext(ctx, name)
			if err == nil {
				t.Fatal("default policy cannot be deleted")
			}
			err = dropApprole(clone, ctx, secretID, path, "myrole")
			if err != nil {
				t.Fatal(err)
			}

			client.SetNamespace("")
			_, err = client.Logical().DeleteWithContext(ctx, "sys/namespaces/"+rootNS)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPolicyNamespaceCustom(t *testing.T) {
	t.Parallel()
	for i, b := range []any{new(raftClusterOpts), new(fileOpts), nil} {
		t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
			client, closer := testVaultServer(t, b)
			defer closer()

			ctx := context.Background()
			rootNS := "pname"
			clone, err := cloneClient(ctx, client, rootNS)
			if err != nil {
				t.Fatal(err)
			}

			sys := clone.Sys()
			logical := clone.Logical()

			name := "default"
			err = sys.PutPolicyWithContext(ctx, name, getDefaultRule())
			if err != nil {
				t.Fatal(err)
			}
			nameCustom := "readpolicy"
			err = sys.PutPolicyWithContext(ctx, nameCustom, getReadApproleRule())
			if err != nil {
				t.Fatal(err)
			}
			policies := []string{name, nameCustom}

			path := "approle"
			_, secretID, clientToken, err := getApprole(clone, ctx, path, "myrole", policies...)
			if err != nil {
				t.Fatal(err)
			}

			rootToken := clone.Token()
			clone.SetToken(clientToken)
			secret, err := logical.ReadWithContext(ctx, "auth/token/lookup-self")
			if err != nil {
				t.Fatal(err)
			}
			if secret == nil || secret.Data == nil {
				t.Errorf("no secret")
			}
			ok := (secret.Data["policies"].([]any)[0].(string) == name &&
				secret.Data["policies"].([]any)[1].(string) == nameCustom) ||
				(secret.Data["policies"].([]any)[0].(string) == nameCustom &&
					secret.Data["policies"].([]any)[1].(string) == name)
			if !ok {
				t.Errorf("%#v", secret.Data)
			}

			secret, err = logical.ReadWithContext(ctx, "auth/approle/role/myrole")
			if err != nil {
				t.Fatal(err)
			}
			if secret == nil || secret.Data == nil {
				t.Errorf("%#v", secret.Data)
			}

			clone.SetToken(rootToken)
			err = dropApprole(clone, ctx, secretID, path, "myrole")
			if err != nil {
				t.Fatal(err)
			}

			err = sys.DeletePolicyWithContext(ctx, name)
			if err == nil {
				t.Fatal("default policy cannot be deleted")
			}
			err = sys.DeletePolicyWithContext(ctx, nameCustom)
			if err != nil {
				t.Fatal(err)
			}
			arr, err := sys.ListPoliciesWithContext(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(arr) != 1 || arr[0] != "default" {
				t.Errorf("%#v", arr)
			}

			client.SetNamespace("")
			_, err = client.Logical().DeleteWithContext(ctx, "sys/namespaces/"+rootNS)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPolicyMixDeleteInNamespace(t *testing.T) {
	t.Parallel()
	for i, b := range []any{new(raftClusterOpts), new(fileOpts), nil} {
		t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
			client, closer := testVaultServer(t, b)
			defer closer()

			ctx := context.Background()
			rootToken := client.Token()
			logical := client.Logical()
			sys := client.Sys()

			rootNS := "pname"
			_, err := logical.WriteWithContext(ctx, "sys/namespaces/"+rootNS, nil)
			if err != nil {
				t.Fatal(err)
			}

			clone, err := client.Clone()
			if err != nil {
				t.Fatal(err)
			}
			clone.SetNamespace(rootNS)
			clone.SetToken(rootToken)
			sysNS := clone.Sys()

			name := "readpolicy"
			policies := []string{name}
			err = sys.PutPolicyWithContext(ctx, name, getReadApproleRule())
			if err != nil {
				t.Fatal(err)
			}

			path := "approle"
			_, secretID, clientToken, err := getApprole(client, ctx, path, "myrole", policies...)
			if err != nil {
				t.Fatal(err)
			}

			client.SetToken(clientToken)
			secret, err := logical.ReadWithContext(ctx, "auth/approle/role/myrole")
			if err != nil {
				t.Fatal(err)
			}
			if secret == nil || secret.Data == nil {
				t.Errorf("%#v", secret.Data)
			}
			if secret.Data["policies"].([]any)[0].(string) != name {
				t.Errorf("%#v", secret.Data)
			}

			// add policy name in namespace
			err = sysNS.PutPolicyWithContext(ctx, name, getReadApproleRule())
			if err != nil {
				t.Fatal(err)
			}

			// delete policy name in namespace
			err = sysNS.DeletePolicyWithContext(ctx, name)
			if err != nil {
				t.Fatal(err)
			}

			// to see if the root namespace is not affected
			secret, err = logical.ReadWithContext(ctx, "auth/approle/role/myrole")
			if err != nil {
				t.Fatal(err)
			}
			if secret == nil || secret.Data == nil {
				t.Errorf("%#v", secret.Data)
			}
			if secret.Data["policies"].([]any)[0].(string) != name {
				t.Errorf("%#v", secret.Data)
			}

			client.SetToken(rootToken)
			err = sys.DeletePolicyWithContext(ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			err = dropApprole(client, ctx, secretID, path, "myrole")
			if err != nil {
				t.Fatal(err)
			}
			_, err = logical.DeleteWithContext(ctx, "sys/namespaces/"+rootNS)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPolicyMixDeleteInRoot(t *testing.T) {
	t.Parallel()
	for i, b := range []any{new(raftClusterOpts), new(fileOpts), nil} {
		t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
			client, closer := testVaultServer(t, b)
			defer closer()

			ctx := context.Background()

			rootToken := client.Token()
			logical := client.Logical()
			sys := client.Sys()

			rootNS := "pname"
			_, err := logical.WriteWithContext(ctx, "sys/namespaces/"+rootNS, nil)
			if err != nil {
				t.Fatal(err)
			}

			clone, err := client.Clone()
			if err != nil {
				t.Fatal(err)
			}
			clone.SetNamespace(rootNS)
			clone.SetToken(rootToken)
			sysNS := clone.Sys()
			logicalNS := clone.Logical()

			name := "readpolicy"
			err = sys.PutPolicyWithContext(ctx, name, getReadApproleRule())
			if err != nil {
				t.Fatal(err)
			}

			// add name in namespace
			err = sysNS.PutPolicyWithContext(ctx, name, getReadApproleRule())
			if err != nil {
				t.Fatal(err)
			}
			nameDefault := "default"
			err = sysNS.PutPolicyWithContext(ctx, nameDefault, getDefaultRule())
			if err != nil {
				t.Fatal(err)
			}
			policies := []string{name, nameDefault}

			path := "approle"
			_, secretID, clientToken, err := getApprole(clone, ctx, path, "myrole", policies...)
			if err != nil {
				t.Fatal(err)
			}

			clone.SetToken(clientToken)
			secret, err := logicalNS.ReadWithContext(ctx, "auth/approle/role/myrole")
			if err != nil {
				t.Fatal(err)
			}
			if secret == nil || secret.Data == nil {
				t.Errorf("%#v", secret.Data)
			}
			ok := (secret.Data["policies"].([]any)[0].(string) == name &&
				secret.Data["policies"].([]any)[1].(string) == nameDefault) ||
				(secret.Data["policies"].([]any)[0].(string) == nameDefault &&
					secret.Data["policies"].([]any)[1].(string) == name)
			if !ok {
				t.Errorf("%#v", secret.Data)
			}

			// delete policy name in root
			err = sys.DeletePolicyWithContext(ctx, name)
			if err != nil {
				t.Fatal(err)
			}

			// to see if the namespace is not affected
			secret, err = logicalNS.ReadWithContext(ctx, "auth/approle/role/myrole")
			if err != nil {
				t.Fatal(err)
			}
			if secret == nil || secret.Data == nil {
				t.Errorf("%#v", secret.Data)
			}
			ok = (secret.Data["policies"].([]any)[0].(string) == name &&
				secret.Data["policies"].([]any)[1].(string) == nameDefault) ||
				(secret.Data["policies"].([]any)[0].(string) == nameDefault &&
					secret.Data["policies"].([]any)[1].(string) == name)
			if !ok {
				t.Errorf("%#v", secret.Data)
			}

			clone.SetToken(rootToken)
			err = sys.DeletePolicyWithContext(ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			err = dropApprole(clone, ctx, secretID, path, "myrole")
			if err != nil {
				t.Fatal(err)
			}
			_, err = logical.DeleteWithContext(ctx, "sys/namespaces/"+rootNS)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func getReadApproleRule() string {
	return `
	path "auth/approle/role/*" {
		capabilities = ["read"]
	}
	`
}

func getDefaultRule() string {
	// the "less" policy is the same as default but without the ability to renew tokens
	return `
		# Allow tokens to look up their own properties
		path "auth/token/lookup-self" {
		    capabilities = ["read"]
		}

		# Allow tokens to renew themselves
		path "auth/token/renew-self" {
		    capabilities = ["update"]
		}

		# Allow tokens to revoke themselves
		path "auth/token/revoke-self" {
		    capabilities = ["update"]
		}

		# Allow a token to look up its own capabilities on a path
		path "sys/capabilities-self" {
		    capabilities = ["update"]
		}

		# Allow a token to look up its own entity by id or name
		path "identity/entity/id/{{identity.entity.id}}" {
		  capabilities = ["read"]
		}
		path "identity/entity/name/{{identity.entity.name}}" {
		  capabilities = ["read"]
		}


		# Allow a token to look up its resultant ACL from all policies. This is useful
		# for UIs. It is an internal path because the format may change at any time
		# based on how the internal ACL features and capabilities change.
		path "sys/internal/ui/resultant-acl" {
		    capabilities = ["read"]
		}

		# Allow a token to renew a lease via lease_id in the request body; old path for
		# old clients, new path for newer
		path "sys/renew" {
		    capabilities = ["update"]
		}
		path "sys/leases/renew" {
		    capabilities = ["update"]
		}

		# Allow looking up lease properties. This requires knowing the lease ID ahead
		# of time and does not divulge any sensitive information.
		path "sys/leases/lookup" {
		    capabilities = ["update"]
		}

		# Allow a token to manage its own cubbyhole
		path "cubbyhole/*" {
		    capabilities = ["create", "read", "update", "delete", "list"]
		}

		# Allow a token to wrap arbitrary values in a response-wrapping token
		path "sys/wrapping/wrap" {
		    capabilities = ["update"]
		}

		# Allow a token to look up the creation time and TTL of a given
		# response-wrapping token
		path "sys/wrapping/lookup" {
		    capabilities = ["update"]
		}

		# Allow a token to unwrap a response-wrapping token. This is a convenience to
		# avoid client token swapping since this is also part of the response wrapping
		# policy.
		path "sys/wrapping/unwrap" {
		    capabilities = ["update"]
		}

		# Allow general purpose tools
		path "sys/tools/hash" {
		    capabilities = ["update"]
		}
		path "sys/tools/hash/*" {
		    capabilities = ["update"]
		}

		# Allow a token to make requests to the Authorization Endpoint for OIDC providers.
		path "identity/oidc/provider/+/authorize" {
		    capabilities = ["read", "update"]
		}
	`
}
