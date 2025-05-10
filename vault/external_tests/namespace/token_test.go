package namespace

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/openbao/openbao/api/v2"
)

func TestTokenRoot(t *testing.T) {
	t.Parallel()
	for i, b := range []any{new(raftClusterOpts), new(fileOpts), nil} {
		t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
			client, closer := testVaultServer(t, b)
			defer closer()

			ctx := context.Background()
			rootToken := client.Token()

			path := "token"
			err := checkTokenAuth(ctx, client, path)
			if err != nil {
				t.Fatal(err)
			}

			tokenAuth, secret, err := getTokenAuthSecret(ctx, client, rootToken, "default")
			if err != nil {
				t.Fatal(err)
			}

			// test revocation
			s, err := revokeTokenByRootToken(ctx, client, tokenAuth, path, rootToken, secret.Auth.ClientToken)
			if err != nil {
				t.Fatal(err)
			}
			if s != nil && s.Auth != nil {
				t.Fatalf("revocation failed %+v", s.Auth)
			}
		})
	}
}

func TestTokenNamespace(t *testing.T) {
	t.Parallel()
	for i, b := range []any{new(raftClusterOpts), new(fileOpts), nil} {
		t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
			client, closer := testVaultServer(t, b)
			defer closer()

			ctx := context.Background()
			rootToken := client.Token()

			rootNS := "pname"
			clone, err := cloneClient(ctx, client, rootNS)
			if err != nil {
				t.Fatal(err)
			}

			path := "token"
			err = checkTokenAuth(ctx, clone, path)
			if err != nil {
				t.Fatal(err)
			}

			tokenAuth1, secret1, err := getTokenAuthSecret(ctx, clone, rootToken, "default")
			if err != nil {
				t.Fatal(err)
			}
			tokenAuth2, secret2, err := getTokenAuthSecret(ctx, clone, rootToken, "default")
			if err != nil {
				t.Fatal(err)
			}

			if client.Token() != clone.Token() ||
				client.Namespace() != "" ||
				clone.Namespace() != rootNS {
				t.Errorf("root Token: %s in namespace %s", client.Token(), client.Namespace())
				t.Errorf("root Token: %s in namespace %s", clone.Token(), clone.Namespace())
			}

			s1, err := revokeTokenByRootToken(ctx, clone, tokenAuth1, path, rootToken, secret1.Auth.ClientToken)
			if err != nil {
				t.Fatal(err)
			}
			if s1 != nil && s1.Auth != nil {
				t.Fatalf("revocation failed %+v", s1.Auth)
			}
			// remove a namespace token from the root namespace ... ok?
			s2, err := revokeTokenByRootToken(ctx, client, tokenAuth2, path, rootToken, secret2.Auth.ClientToken)
			if err != nil {
				t.Fatal(err)
			}
			if s2 != nil && s2.Auth != nil {
				t.Fatalf("revocation failed %+v", s2.Auth)
			}

			client.SetNamespace("")
			_, err = client.Logical().DeleteWithContext(ctx, "sys/namespaces/"+rootNS)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTokenMix(t *testing.T) {
	t.Parallel()
	for i, b := range []any{new(raftClusterOpts), new(fileOpts), nil} {
		t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
			client, closer := testVaultServer(t, b)
			defer closer()

			ctx := context.Background()
			rootToken := client.Token()

			path := "token"

			// in root namespace
			err := checkTokenAuth(ctx, client, path)
			if err != nil {
				t.Fatal(err)
			}
			tokenAuth1, secret1, err := getTokenAuthSecret(ctx, client, rootToken, "default")
			if err != nil {
				t.Fatal(err)
			}
			tokenAuth3, secret3, err := getTokenAuthSecret(ctx, client, rootToken, "default")
			if err != nil {
				t.Fatal(err)
			}

			// in namespace
			rootNS := "pname"
			clone, err := cloneClient(ctx, client, rootNS)
			if err != nil {
				t.Fatal(err)
			}

			err = checkTokenAuth(ctx, clone, path)
			if err != nil {
				t.Fatal(err)
			}
			tokenAuth2, secret2, err := getTokenAuthSecret(ctx, clone, rootToken, "default")
			if err != nil {
				t.Fatal(err)
			}
			tokenAuth4, secret4, err := getTokenAuthSecret(ctx, clone, rootToken, "default")
			if err != nil {
				t.Fatal(err)
			}

			if client.Token() != clone.Token() ||
				client.Namespace() != "" ||
				clone.Namespace() != rootNS {
				t.Errorf("root Token: %s in namespace %s", client.Token(), client.Namespace())
				t.Errorf("root Token: %s in namespace %s", clone.Token(), clone.Namespace())
			}

			s1, err := revokeTokenByRootToken(ctx, client, tokenAuth1, path, rootToken, secret1.Auth.ClientToken)
			if err != nil {
				t.Fatal(err)
			}
			if s1 != nil && s1.Auth != nil {
				t.Fatalf("revocation failed %+v", s1.Auth)
			}
			s2, err := revokeTokenByRootToken(ctx, clone, tokenAuth2, path, rootToken, secret2.Auth.ClientToken)
			if err != nil {
				t.Fatal(err)
			}
			if s2 != nil && s2.Auth != nil {
				t.Fatalf("revocation failed %+v", s2.Auth)
			}

			// remove a normal token in space from the namespace ... ok?
			s3, err := revokeTokenByRootToken(ctx, clone, tokenAuth3, path, rootToken, secret3.Auth.ClientToken)
			if err != nil {
				t.Fatal(err)
			}
			if s3 != nil && s3.Auth != nil {
				t.Fatalf("revocation failed %+v", s3.Auth)
			}

			// remove a namespace token from the root namespace ... ok?
			s4, err := revokeTokenByRootToken(ctx, client, tokenAuth4, path, rootToken, secret4.Auth.ClientToken)
			if err != nil {
				t.Fatal(err)
			}
			if s4 != nil && s4.Auth != nil {
				t.Fatalf("revocation failed %+v", s4.Auth)
			}

			// clean up
			client.SetNamespace("")
			_, err = client.Logical().DeleteWithContext(ctx, "sys/namespaces/"+rootNS)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func checkTokenAuth(ctx context.Context, client *api.Client, path string) error {
	sys := client.Sys()

	mountsRspn, err := sys.ListAuthWithContext(ctx)
	if err != nil {
		return err
	}
	for k, rspn := range mountsRspn {
		if !slices.Contains([]string{"token/"}, k) {
			return fmt.Errorf("wrong mount response: %s => %+v", k, rspn)
		}
	}

	err = sys.DisableAuthWithContext(ctx, path)
	if err == nil {
		return fmt.Errorf("Should have failed")
	} else if rErr, ok := err.(*api.ResponseError); !ok || rErr.StatusCode != 400 || (rErr.Errors)[0] != "token credential backend cannot be disabled" {
		return fmt.Errorf("%#v", rErr.Errors)
	}
	return nil
}

func getTokenAuthSecret(ctx context.Context, client *api.Client, rootToken, policy string) (*api.TokenAuth, *api.Secret, error) {
	client.SetToken(rootToken)

	tokenAuth := client.Auth().Token()
	secret, err := tokenAuth.CreateWithContext(ctx, &api.TokenCreateRequest{
		Policies: []string{policy},
	})
	if err != nil {
		return nil, nil, err
	}
	if secret.Auth == nil || secret.Auth.ClientToken == "" {
		return nil, nil, fmt.Errorf("Auth data: %+v", secret.Auth)
	}
	token := secret.Auth.ClientToken
	client.SetToken(token)
	tokenAuth = client.Auth().Token()

	selfSecret, err := tokenAuth.LookupSelfWithContext(ctx)
	if err != nil {
		return nil, nil, err
	}
	if selfSecret == nil || selfSecret.Data == nil || selfSecret.Data["policies"] == nil {
		return nil, nil, fmt.Errorf("self response data nil: %+v", selfSecret)
	}
	policies := selfSecret.Data["policies"].([]any)
	if policies[0].(string) != policy {
		return nil, nil, fmt.Errorf("Policy %s not found.", policies)
	}

	client.SetToken(rootToken)
	return tokenAuth, secret, nil
}

func revokeTokenByRootToken(ctx context.Context, client *api.Client, tokenAuth *api.TokenAuth, path, rootToken, token string) (*api.Secret, error) {
	client.SetToken(rootToken)

	secret, err := client.Logical().WriteWithContext(ctx, "auth/"+path+"/revoke", map[string]any{
		"token": token,
	})
	if err != nil {
		return nil, err
	}
	if secret != nil {
		return nil, fmt.Errorf("secret found after revoke api: %+v", secret)
	}

	s, err := tokenAuth.LookupSelfWithContext(ctx)
	if err != nil {
		if rErr, ok := err.(*api.ResponseError); !ok || rErr.StatusCode != 403 || (rErr.Errors)[0] != "permission denied" {
			return nil, fmt.Errorf("error: %#v", rErr.Errors)
		}
	}
	return s, nil
}
