package namespace

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/openbao/openbao/api/v2"
)

func TestACLRoot(t *testing.T) {
	t.Parallel()
	for _, f := range []func(context.Context, *api.Client, string, string, string, string, string, string) (string, string, error){
		getTokensUserpass,
		getTokensClient,
	} {
		for i, b := range []any{new(raftClusterOpts), new(fileOpts), nil} {
			time.Sleep(2 * time.Second)
			t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
				client, closer := testUserpassServer(t, b)
				defer closer()

				ctx := context.Background()
				rootToken := client.Token()

				path := "mountPath"
				title := "mysecret"
				err := createKV2Secret(ctx, client, path, title, "myadmin", "123456")
				if err != nil {
					t.Fatal(err)
				}

				readACL := "userread"
				writeACL := "userwrite"
				userToken1, userToken2, err := f(ctx, client, rootToken, path, readACL, writeACL, getKV2Read(path), getKV2Write(path))
				if err != nil {
					t.Fatal(err)
				}

				// userToken1 can read "mysecret"
				client.SetToken(userToken1)
				err = canReadNotWriteTitle(ctx, client, path, title)
				if err != nil {
					t.Fatal(err)
				}

				// userToken2 can read "mysecret"
				client.SetToken(userToken2)
				err = canReadAndWriteTitle(ctx, client, path, title)
				if err != nil {
					t.Fatal(err)
				}

				// cleanup
				client.SetToken(rootToken)
				err = client.Sys().UnmountWithContext(ctx, path)
				if err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestACLNamespace(t *testing.T) {
	t.Parallel()
	for _, f := range []func(context.Context, *api.Client, string, string, string, string, string, string) (string, string, error){
		getTokensUserpass,
		getTokensClient,
	} {
		for i, b := range []any{new(raftClusterOpts), new(fileOpts), nil} {
			time.Sleep(2 * time.Second)
			t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
				client, closer := testUserpassServer(t, b)
				defer closer()

				ctx := context.Background()
				rootToken := client.Token()

				rootNS := "ns1"
				clone, err := cloneClient(ctx, client, rootNS)
				if err != nil {
					t.Fatal(err)
				}

				path := "mountPath"
				title := "mysecret"
				err = createKV2Secret(ctx, clone, path, title, "myadmin", "123456")
				if err != nil {
					t.Fatal(err)
				}

				readACL := "userread"
				writeACL := "userwrite"
				userToken1, userToken2, err := f(ctx, clone, rootToken, path, readACL, writeACL, getKV2Read(path), getKV2Write(path))
				if err != nil {
					t.Fatal(err)
				}

				// userToken1 can read "mysecret"
				clone.SetToken(userToken1)
				err = canReadNotWriteTitle(ctx, clone, path, title)
				if err != nil {
					t.Fatal(err)
				}

				// userToken2 can read "mysecret"
				clone.SetToken(userToken2)
				err = canReadAndWriteTitle(ctx, clone, path, title)
				if err != nil {
					t.Fatal(err)
				}

				// cleanup
				clone.SetToken(rootToken)
				err = clone.Sys().UnmountWithContext(ctx, path)
				if err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestACLMixNormal(t *testing.T) {
	t.Parallel()
	for _, f := range []func(context.Context, *api.Client, string, string, string, string, string, string) (string, string, error){
		getTokensUserpass,
		getTokensClient,
	} {
		for i, b := range []any{new(raftClusterOpts), new(fileOpts), nil} {
			time.Sleep(2 * time.Second)
			t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
				client, closer := testUserpassServer(t, b)
				defer closer()

				ctx := context.Background()
				rootToken := client.Token()

				// in default namespace
				path := "mountPath"
				title := "mysecret"
				err := createKV2Secret(ctx, client, path, title, "myadmin", "123456")
				if err != nil {
					t.Fatal(err)
				}
				readACL := "userread"
				writeACL := "userwrite"
				userToken1, userToken2, err := f(ctx, client, rootToken, path, readACL, writeACL, getKV2Read(path), getKV2Write(path))
				if err != nil {
					t.Fatal(err)
				}

				// in namespace
				rootNS := "ns1"
				clone, err := cloneClient(ctx, client, rootNS)
				if err != nil {
					t.Fatal(err)
				}
				err = createKV2Secret(ctx, clone, path, title, "myadmin", "123456")
				userToken3, userToken4, err := f(ctx, clone, rootToken, path, readACL, writeACL, getKV2Read(path), getKV2Write(path))
				if err != nil {
					t.Fatal(err)
				}

				// in sub namespace
				subNS := "ns2"
				clone2, err := cloneClient(ctx, clone, subNS)
				if err != nil {
					t.Fatal(err)
				}
				err = createKV2Secret(ctx, clone2, path, title, "myadmin", "123456")
				userToken5, userToken6, err := f(ctx, clone2, rootToken, path, readACL, writeACL, getKV2Read(path), getKV2Write(path))
				if err != nil {
					t.Fatal(err)
				}

				// IN THE DEFAULT NAMESPACE

				// userToken1 can read the default namespace
				client.SetToken(userToken1)
				err = canReadNotWriteTitle(ctx, client, path, title)
				if err != nil {
					t.Fatal(err)
				}
				// userToken3 can not read the default namespace
				client.SetToken(userToken3)
				err = canReadNotWriteTitle(ctx, client, path, title)
				if err == nil || !strings.Contains(err.Error(), "permission denied") {
					t.Fatal(err)
				}
				// userToken5 can not read the default namespace
				client.SetToken(userToken5)
				err = canReadNotWriteTitle(ctx, client, path, title)
				if err == nil || !strings.Contains(err.Error(), "permission denied") {
					t.Fatal(err)
				}

				// userToken2 can write the default namespace
				client.SetToken(userToken2)
				err = canReadAndWriteTitle(ctx, client, path, title)
				if err != nil {
					t.Fatal(err)
				}
				// userToken4 can not write the default namespace
				client.SetToken(userToken4)
				err = canReadNotWriteTitle(ctx, client, path, title)
				if err == nil || !strings.Contains(err.Error(), "permission denied") {
					t.Fatal(err)
				}
				// userToken6 can not write the default namespace
				client.SetToken(userToken6)
				err = canReadNotWriteTitle(ctx, client, path, title)
				if err == nil || !strings.Contains(err.Error(), "permission denied") {
					t.Fatal(err)
				}

				// IN NAMESPACE ns1

				// userToken1 can not read the ns1 namespace
				clone.SetToken(userToken1)
				err = canReadNotWriteTitle(ctx, clone, path, title)
				if err == nil || !strings.Contains(err.Error(), "permission denied") {
					t.Fatal(err)
				}
				// userToken3 can read the ns1 namespace
				clone.SetToken(userToken3)
				err = canReadNotWriteTitle(ctx, clone, path, title)
				if err != nil {
					t.Fatal(err)
				}
				// userToken5 can not read the ns1 namespace
				clone.SetToken(userToken5)
				err = canReadNotWriteTitle(ctx, clone, path, title)
				if err == nil || !strings.Contains(err.Error(), "permission denied") {
					t.Fatal(err)
				}

				// userToken2 can not write the ns1 namespace
				clone.SetToken(userToken2)
				err = canReadNotWriteTitle(ctx, clone, path, title)
				if err == nil || !strings.Contains(err.Error(), "permission denied") {
					t.Fatal(err)
				}
				// userToken4 can write the ns1 namespace
				clone.SetToken(userToken4)
				err = canReadAndWriteTitle(ctx, clone, path, title)
				if err != nil {
					t.Fatal(err)
				}
				// userToken6 can not write the ns1 namespace
				clone.SetToken(userToken6)
				err = canReadNotWriteTitle(ctx, clone, path, title)
				if err == nil || !strings.Contains(err.Error(), "permission denied") {
					t.Fatal(err)
				}

				// IN NAMESPACE ns1/ns2

				// userToken1 can not read the ns1/ns2 namespace
				clone2.SetToken(userToken1)
				err = canReadNotWriteTitle(ctx, clone2, path, title)
				if err == nil || !strings.Contains(err.Error(), "permission denied") {
					t.Fatal(err)
				}
				// userToken3 can not read the ns1/ns2 namespace
				clone2.SetToken(userToken3)
				err = canReadNotWriteTitle(ctx, clone2, path, title)
				if err == nil || !strings.Contains(err.Error(), "permission denied") {
					t.Fatal(err)
				}
				// userToken5 can read the ns1/ns2 namespace
				clone2.SetToken(userToken5)
				err = canReadNotWriteTitle(ctx, clone2, path, title)
				if err != nil {
					t.Fatal(err)
				}
				// userToken2 can not write the ns1/ns2 namespace
				clone2.SetToken(userToken2)
				err = canReadNotWriteTitle(ctx, clone2, path, title)
				if err == nil || !strings.Contains(err.Error(), "permission denied") {
					t.Fatal(err)
				}
				// userToken4 can not write the ns1/ns2 namespace
				clone2.SetToken(userToken4)
				err = canReadNotWriteTitle(ctx, clone2, path, title)
				if err == nil || !strings.Contains(err.Error(), "permission denied") {
					t.Fatal(err)
				}
				// userToken6 can write the ns1/ns2 namespace
				clone2.SetToken(userToken6)
				err = canReadAndWriteTitle(ctx, clone2, path, title)
				if err != nil {
					t.Fatal(err)
				}

				// cleanup
				client.SetToken(rootToken)
				err = client.Sys().UnmountWithContext(ctx, path)
				if err != nil {
					t.Fatal(err)
				}
				clone.SetToken(rootToken)
				err = clone.Sys().UnmountWithContext(ctx, path)
				if err != nil {
					t.Fatal(err)
				}
				clone2.SetToken(rootToken)
				err = clone2.Sys().UnmountWithContext(ctx, path)
				if err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestACLMixPower(t *testing.T) {
	t.Parallel()
	for _, f := range []func(context.Context, *api.Client, string, string, string, string, string, string) (string, string, error){
		getTokensUserpass,
		getTokensClient,
	} {
		for i, b := range []any{new(raftClusterOpts), new(fileOpts), nil} {
			time.Sleep(2 * time.Second)
			t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
				client, closer := testUserpassServer(t, b)
				defer closer()

				ctx := context.Background()
				rootToken := client.Token()

				// in default namespace
				path := "mountPath"
				title := "mysecret"
				err := createKV2Secret(ctx, client, path, title, "myadmin", "123456")
				if err != nil {
					t.Fatal(err)
				}
				readACL := "userread"
				writeACL := "userwrite"
				userToken1, userToken2, err := f(ctx, client, rootToken, path, readACL, writeACL, getPowerRead(path), getPowerWrite(path))
				if err != nil {
					t.Fatal(err)
				}

				// in namespace
				rootNS := "ns1"
				clone, err := cloneClient(ctx, client, rootNS)
				if err != nil {
					t.Fatal(err)
				}
				err = createKV2Secret(ctx, clone, path, title, "myadmin", "123456")
				userToken3, userToken4, err := f(ctx, clone, rootToken, path, readACL, writeACL, getPowerRead(path), getPowerWrite(path))
				if err != nil {
					t.Fatal(err)
				}

				// in sub namespace
				subNS := "ns2"
				clone2, err := cloneClient(ctx, clone, subNS)
				if err != nil {
					t.Fatal(err)
				}
				err = createKV2Secret(ctx, clone2, path, title, "myadmin", "123456")
				userToken5, userToken6, err := f(ctx, clone2, rootToken, path, readACL, writeACL, getPowerRead(path), getPowerWrite(path))
				if err != nil {
					t.Fatal(err)
				}

				// IN THE DEFAULT NAMESPACE

				// userToken1 can read the default namespace
				client.SetToken(userToken1)
				err = canReadNotWriteTitle(ctx, client, path, title)
				if err != nil {
					t.Fatal(err)
				}
				// userToken3 can not read the default namespace
				client.SetToken(userToken3)
				err = canReadNotWriteTitle(ctx, client, path, title)
				if err == nil || !strings.Contains(err.Error(), "permission denied") {
					t.Fatal(err)
				}
				// userToken5 can not read the default namespace
				client.SetToken(userToken5)
				err = canReadNotWriteTitle(ctx, client, path, title)
				if err == nil || !strings.Contains(err.Error(), "permission denied") {
					t.Fatal(err)
				}

				// userToken2 can write the default namespace
				client.SetToken(userToken2)
				err = canReadAndWriteTitle(ctx, client, path, title)
				if err != nil {
					t.Fatal(err)
				}
				// userToken4 can not write the default namespace
				client.SetToken(userToken4)
				err = canReadNotWriteTitle(ctx, client, path, title)
				if err == nil || !strings.Contains(err.Error(), "permission denied") {
					t.Fatal(err)
				}
				// userToken6 can not write the default namespace
				client.SetToken(userToken6)
				err = canReadNotWriteTitle(ctx, client, path, title)
				if err == nil || !strings.Contains(err.Error(), "permission denied") {
					t.Fatal(err)
				}

				// IN NAMESPACE ns1

				// userToken1 can read the ns1 namespace
				clone.SetToken(userToken1)
				err = canReadNotWriteTitle(ctx, clone, path, title)
				if err != nil {
					t.Fatal(err)
				}
				// userToken3 can read the ns1 namespace
				clone.SetToken(userToken3)
				err = canReadNotWriteTitle(ctx, clone, path, title)
				if err != nil {
					t.Fatal(err)
				}
				// userToken5 can not read the ns1 namespace
				clone.SetToken(userToken5)
				err = canReadNotWriteTitle(ctx, clone, path, title)
				if err == nil || !strings.Contains(err.Error(), "permission denied") {
					t.Fatal(err)
				}

				// userToken2 can write the ns1 namespace
				clone.SetToken(userToken2)
				err = canReadNotWriteTitle(ctx, clone, path, title)
				if err != nil {
					t.Fatal(err)
				}
				// userToken4 can write the ns1 namespace
				clone.SetToken(userToken4)
				err = canReadAndWriteTitle(ctx, clone, path, title)
				if err != nil {
					t.Fatal(err)
				}
				// userToken6 can not write the ns1 namespace
				clone.SetToken(userToken6)
				err = canReadNotWriteTitle(ctx, clone, path, title)
				if err == nil || !strings.Contains(err.Error(), "permission denied") {
					t.Fatal(err)
				}

				// IN NAMESPACE ns1/ns2

				// userToken1 can not read the ns1/ns2 namespace
				clone2.SetToken(userToken1)
				err = canReadNotWriteTitle(ctx, clone2, path, title)
				if err == nil || !strings.Contains(err.Error(), "permission denied") {
					t.Fatal(err)
				}
				// userToken3 can read the ns1/ns2 namespace
				clone2.SetToken(userToken3)
				err = canReadNotWriteTitle(ctx, clone2, path, title)
				if err != nil {
					t.Fatal(err)
				}
				// userToken5 can read the ns1/ns2 namespace
				clone2.SetToken(userToken5)
				err = canReadNotWriteTitle(ctx, clone2, path, title)
				if err != nil {
					t.Fatal(err)
				}
				// userToken2 can not write the ns1/ns2 namespace
				clone2.SetToken(userToken2)
				err = canReadNotWriteTitle(ctx, clone2, path, title)
				if err == nil || !strings.Contains(err.Error(), "permission denied") {
					t.Fatal(err)
				}
				// userToken4 can write the ns1/ns2 namespace
				clone2.SetToken(userToken4)
				err = canReadNotWriteTitle(ctx, clone2, path, title)
				if err != nil {
					t.Fatal(err)
				}
				// userToken6 can write the ns1/ns2 namespace
				clone2.SetToken(userToken6)
				err = canReadAndWriteTitle(ctx, clone2, path, title)
				if err != nil {
					t.Fatal(err)
				}

				// cleanup
				client.SetToken(rootToken)
				err = client.Sys().UnmountWithContext(ctx, path)
				if err != nil {
					t.Fatal(err)
				}
				clone.SetToken(rootToken)
				err = clone.Sys().UnmountWithContext(ctx, path)
				if err != nil {
					t.Fatal(err)
				}
				clone2.SetToken(rootToken)
				err = clone2.Sys().UnmountWithContext(ctx, path)
				if err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

// getKV2Read returns the ACL policy for reading KV2 secrets.
func getKV2Read(path string) string {
	return `
	# Allow user to read config
	path "` + path + `/config/*" {
		capabilities = ["read"]
	}
	# Allow user to read metadata, and list secrets
	path "` + path + `/metadata/*" {
		capabilities = ["read", "list"]
	}
	# Allow user to read secrets
	path "` + path + `/data/*" {
		capabilities = ["read"]
	}
	`
}

// getKV2Write returns the ACL policy for writing KV2 secrets.
func getKV2Write(path string) string {
	return `
	# Allow user to read config
	path "` + path + `/config/*" {
		capabilities = ["read"]
	}
	# Allow user to read metadata, and list secrets
	path "` + path + `/metadata/*" {
		capabilities = ["read", "list"]
	}
	# Allow user to read, create, update, delete secrets
	path "` + path + `/data/*" {
		capabilities = ["read", "create", "update", "delete"]
	}
	`
}

// getPowerRead returns the ACL policy for reading KV2 secrets, and in sub namespaces
func getPowerRead(path string) string {
	return `
	# Allow user to read config
	path "` + path + `/config/*" {
		capabilities = ["read"]
	}
	# Allow user to read metadata, and list secrets
	path "` + path + `/metadata/*" {
		capabilities = ["read", "list"]
	}
	# Allow user to read secrets
	path "` + path + `/data/*" {
		capabilities = ["read"]
	}
	# Allow user to read secrets in sub namespaces
	path "+/` + path + `/data/*" {
		capabilities = ["read"]
	}
	`
}

// getPowerWrite returns the ACL policy for writing KV2 secrets, and in sub namespaces
func getPowerWrite(path string) string {
	return `
	# Allow user to read config
	path "` + path + `/config/*" {
		capabilities = ["read"]
	}
	# Allow user to read metadata, and list secrets
	path "` + path + `/metadata/*" {
		capabilities = ["read", "list"]
	}
	# Allow user to read, create, update, delete secrets
	path "` + path + `/data/*" {
		capabilities = ["read", "create", "update", "delete"]
	}
	# Allow user to read, create, update, delete secrets in sub namespaces
	path "+/` + path + `/data/*" {
		capabilities = ["read", "create", "update", "delete"]
	}
	`
}

// createKV2Secret mounts a KV2 secrets engine at the given path and creates a secret
// with the given title and data. It returns the KV2 secrets engine and the secret.
func createKV2Secret(ctx context.Context, client *api.Client, path, title, username, password string) error {
	err := checkKVMount(ctx, client, path)
	if err != nil {
		return err
	}

	_, err = createGetKV2(ctx, client, path, title, username, password)
	return err
}

// canReadNotWriteTitle checks if the client can read the secret with the given path and title.
func canReadNotWriteTitle(ctx context.Context, client *api.Client, path, title string) error {
	kv2 := client.KVv2(path)
	// can read
	kvSecret, err := kv2.Get(ctx, title)
	if err != nil {
		return err
	}
	if kvSecret.Data == nil ||
		kvSecret.Data["username"].(string) != "myadmin" ||
		kvSecret.Data["password"].(string) != "123456" {
		return fmt.Errorf("KV secret: %#v", kvSecret.Data)
	}
	// no write
	kvSecret, err = kv2.Put(ctx, title+"1", map[string]any{
		"username": "myadmin1",
		"password": "1234567",
	})
	if err == nil || !strings.Contains(err.Error(), "permission denied") {
		return err
	}
	return nil
}

// canReadAndWriteTitle checks if the client can read and write the secret with the given path and title.
func canReadAndWriteTitle(ctx context.Context, client *api.Client, path, title string) error {
	kv2 := client.KVv2(path)
	// can read
	kvSecret, err := kv2.Get(ctx, title)
	if err != nil {
		return err
	}
	if kvSecret.Data == nil ||
		kvSecret.Data["username"].(string) != "myadmin" ||
		kvSecret.Data["password"].(string) != "123456" {
		return fmt.Errorf("KV secret: %#v", kvSecret.Data)
	}
	// can write
	kvSecret, err = kv2.Put(ctx, title+"1", map[string]any{
		"username": "myadmin1",
		"password": "1234567",
	})
	if err != nil {
		return err
	}

	kvSecret, err = kv2.Get(ctx, title+"1")
	if err != nil {
		return err
	}
	if kvSecret.Data == nil ||
		kvSecret.Data["username"].(string) != "myadmin1" ||
		kvSecret.Data["password"].(string) != "1234567" {
		return fmt.Errorf("KV secret: %#v", kvSecret.Data)
	}
	return nil
}

// getTokensClient creates a read policy and a write policy at the path
// using the client which is associated with the namespace. Then it creates a read and a write token.
// It returns the two tokens.
func getTokensClient(ctx context.Context, client *api.Client, rootToken, path, readACL, writeACL, readBody, writeBody string) (string, string, error) {
	// create a read policy at the path using readBody
	err := client.Sys().PutPolicyWithContext(ctx, readACL, readBody)
	if err != nil {
		return "", "", err
	}
	// create a write policy at the path using writeBody
	err = client.Sys().PutPolicyWithContext(ctx, writeACL, writeBody)
	if err != nil {
		return "", "", err
	}

	_, secret1, err := getTokenAuthSecret(ctx, client, rootToken, readACL)
	if err != nil {
		return "", "", err
	}
	userToken1 := secret1.Auth.ClientToken
	_, secret2, err := getTokenAuthSecret(ctx, client, rootToken, writeACL)
	if err != nil {
		return "", "", err
	}
	userToken2 := secret2.Auth.ClientToken

	return userToken1, userToken2, nil
}

// getTokensUserpass creates a read policy and a write policy at the path
// using the client which is associated with the namespace. Then it creates a read and a write token.
// It returns the two tokens.
func getTokensUserpass(ctx context.Context, client *api.Client, rootToken, path, readACL, writeACL, readBody, writeBody string) (string, string, error) {
	// create a read policy at the path using readBody
	err := client.Sys().PutPolicyWithContext(ctx, readACL, readBody)
	if err != nil {
		return "", "", err
	}
	// create a write policy at the path using writeBody
	err = client.Sys().PutPolicyWithContext(ctx, writeACL, writeBody)
	if err != nil {
		return "", "", err
	}

	err = client.Sys().EnableAuthWithOptionsWithContext(ctx, path, &api.EnableAuthOptions{
		Type: "userpass",
	})
	if err != nil {
		return "", "", err
	}
	userToken1, err := getUserpassSecret(ctx, client, rootToken, path, readACL)
	if err != nil {
		return "", "", err
	}

	userToken2, err := getUserpassSecret(ctx, client, rootToken, path, writeACL)

	return userToken1, userToken2, err
}

// getUserpassSecret creates a new token from client, which is associated with a namespace, with the given policies and returns the token auth and secret.
func getUserpassSecret(ctx context.Context, client *api.Client, rootToken, path string, policy ...string) (string, error) {
	client.SetToken(rootToken)

	secret, err := client.Logical().Write("auth/"+path+"/users/user", map[string]any{
		"password":       "pass",
		"token_policies": policy,
	})
	if err != nil {
		return "", err
	}
	if secret != nil {
		return "", fmt.Errorf("secret found after create user api: %+v", secret)
	}

	rspn, err := client.Logical().ListWithContext(ctx, "auth/"+path+"/users")
	if err != nil {
		return "", err
	}
	if rspn == nil || rspn.Data == nil || rspn.Data["keys"] == nil || len(rspn.Data["keys"].([]any)) != 1 {
		return "", fmt.Errorf("list response data nil: %+v", rspn)
	}
	if rspn.Data["keys"].([]any)[0].(string) != "user" {
		return "", fmt.Errorf("user not found in %v.", rspn.Data["keys"])
	}

	secret, err = client.Logical().WriteWithContext(ctx, "auth/"+path+"/login/user", map[string]any{
		"password": "pass",
	})
	if err != nil {
		return "", err
	}
	if secret == nil || secret.Auth == nil || secret.Auth.ClientToken == "" {
		return "", fmt.Errorf("Auth data: %+v", secret.Auth)
	}
	token := secret.Auth.ClientToken

	client.SetToken(rootToken)
	return token, nil
}
