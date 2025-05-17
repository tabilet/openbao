// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package namespace

import (
	"context"
	"encoding/base64"
	"testing"

	log "github.com/hashicorp/go-hclog"
	"github.com/openbao/openbao/api/v2"
	"github.com/openbao/openbao/audit"
	auditFile "github.com/openbao/openbao/builtin/audit/file"
	credUserpass "github.com/openbao/openbao/builtin/credential/userpass"
	"github.com/openbao/openbao/builtin/logical/database"
	logicalKv "github.com/openbao/openbao/builtin/logical/kv"
	"github.com/openbao/openbao/builtin/logical/pki"
	"github.com/openbao/openbao/builtin/logical/transit"
	"github.com/openbao/openbao/helper/benchhelpers"
	"github.com/openbao/openbao/helper/builtinplugins"
	"github.com/openbao/openbao/helper/testhelpers/corehelpers"
	"github.com/openbao/openbao/helper/testhelpers/pluginhelpers"
	"github.com/openbao/openbao/helper/testhelpers/teststorage"
	"github.com/openbao/openbao/http"
	vaulthttp "github.com/openbao/openbao/http"
	"github.com/openbao/openbao/sdk/v2/helper/consts"
	"github.com/openbao/openbao/sdk/v2/logical"
	"github.com/openbao/openbao/vault"
)

func cloneClient(ctx context.Context, client *api.Client, pname string) (*api.Client, error) {
	_, err := client.Logical().WriteWithContext(ctx, "sys/namespaces/"+pname, nil)
	if err != nil {
		return nil, err
	}
	clone, err := client.Clone()
	if err != nil {
		return nil, err
	}
	clone.SetToken(client.Token())
	top := client.Namespace()
	if top == "" {
		clone.SetNamespace(pname)
	} else {
		clone.SetNamespace(top + "/" + pname)
	}
	return clone, nil
}

func testPluginServer(t *testing.T, rest ...any) (*api.Client, pluginhelpers.TestPlugin, func()) {
	t.Helper()

	pluginDir, cleanup := corehelpers.MakeTestPluginDir(t)
	t.Cleanup(func() { cleanup(t) })
	coreConfig := &vault.CoreConfig{
		PluginDirectory: pluginDir,
		LogicalBackends: map[string]logical.Factory{
			"database": database.Factory,
		},
	}
	opts := &vault.TestClusterOptions{
		TempDir:  pluginDir,
		NumCores: 1,
		Plugins: &vault.TestPluginConfig{
			Typ:      consts.PluginTypeSecrets,
			Versions: []string{""},
		},
		HandlerFunc: vaulthttp.Handler,
	}

	cluster, client, _, closer := testVaultServerCoreConfigOpts(t, coreConfig, opts, rest...)
	return client, cluster.Plugins[0], closer
}

// testUserpassServer creates a test vault cluster with the KV backend and returns
// a configured API client and closer function.
func testUserpassServer(t testing.TB, rest ...any) (*api.Client, func()) {
	t.Helper()

	// initialize test cluster
	coreConfig := &vault.CoreConfig{
		CredentialBackends: map[string]logical.Factory{
			"userpass": credUserpass.Factory,
		},
		LogicalBackends: map[string]logical.Factory{
			"kv":    logicalKv.Factory,
			"kv-v2": logicalKv.VersionedKVFactory,
		},
	}
	opts := &vault.TestClusterOptions{
		HandlerFunc: http.Handler,
		NumCores:    1,
	}

	_, client, _, closer := testVaultServerCoreConfigOpts(t, coreConfig, opts, rest...)
	return client, closer
}

// testKVServer creates a test vault cluster with the KV backend and returns
// a configured API client and closer function.
func testKVServer(t testing.TB, rest ...any) (*api.Client, func()) {
	t.Helper()

	// initialize test cluster
	coreConfig := &vault.CoreConfig{
		LogicalBackends: map[string]logical.Factory{
			"kv":    logicalKv.Factory,
			"kv-v2": logicalKv.VersionedKVFactory,
		},
	}
	opts := &vault.TestClusterOptions{
		HandlerFunc: http.Handler,
		NumCores:    1,
	}

	_, client, _, closer := testVaultServerCoreConfigOpts(t, coreConfig, opts, rest...)
	return client, closer
}

// testVaultServer creates a test vault cluster and returns a configured API
// client and closer function.
func testVaultServer(t testing.TB, rest ...any) (*api.Client, func()) {
	t.Helper()

	coreConfig := &vault.CoreConfig{
		DisableCache: true,
		Logger:       log.NewNullLogger(),
		CredentialBackends: map[string]logical.Factory{
			"userpass": credUserpass.Factory,
		},
		AuditBackends: map[string]audit.Factory{
			"file": auditFile.Factory,
		},
		LogicalBackends: map[string]logical.Factory{
			"database":       database.Factory,
			"generic-leased": vault.LeasedPassthroughBackendFactory,
			"pki":            pki.Factory,
			"transit":        transit.Factory,
		},
		BuiltinRegistry: builtinplugins.Registry,
	}
	opts := &vault.TestClusterOptions{
		HandlerFunc: http.Handler,
		NumCores:    1,
	}

	_, client, _, closer := testVaultServerCoreConfigOpts(t, coreConfig, opts, rest...)
	return client, closer
}

// testVaultServerCoreConfigOpts creates a test vault cluster and returns a configured
// API client, list of unseal keys (as strings), and a closer function.
func testVaultServerCoreConfigOpts(t testing.TB, coreConfig *vault.CoreConfig, opts *vault.TestClusterOptions, rest ...any) (*vault.TestCluster, *api.Client, []string, func()) {
	t.Helper()

	if len(rest) > 0 {
		coreConfig, opts = modifyBackends(coreConfig, opts, rest[0])
	}

	cluster := vault.NewTestCluster(benchhelpers.TBtoT(t), coreConfig, opts)
	cluster.Start()

	// Make it easy to get access to the active
	core := cluster.Cores[0].Core
	vault.TestWaitActive(benchhelpers.TBtoT(t), core)

	// Get the client already setup for us!
	client := cluster.Cores[0].Client
	client.SetToken(cluster.RootToken)

	// Convert the unseal keys to base64 encoded, since these are how the user
	// will get them.
	unsealKeys := make([]string, len(cluster.BarrierKeys))
	for i := range unsealKeys {
		unsealKeys[i] = base64.StdEncoding.EncodeToString(cluster.BarrierKeys[i])
	}

	return cluster, client, unsealKeys, func() { defer cluster.Cleanup() }
}

type fileOpts struct {
	EnableAutopilot        bool
	PhysicalFactoryConfig  map[string]interface{}
	NumCores               int
	Seal                   vault.Seal
	VersionMap             map[int]string
	EffectiveSDKVersionMap map[int]string
}

func (f fileOpts) reset(coreConfig *vault.CoreConfig, opts *vault.TestClusterOptions) (*vault.CoreConfig, *vault.TestClusterOptions) {
	opts.PhysicalFactoryConfig = f.PhysicalFactoryConfig
	if f.NumCores == 0 && opts.NumCores == 0 {
		opts.NumCores = 1
	} else if f.NumCores > 0 {
		opts.NumCores = f.NumCores
	}
	opts.VersionMap = f.VersionMap
	opts.EffectiveSDKVersionMap = f.EffectiveSDKVersionMap

	coreConfig.DisableAutopilot = !f.EnableAutopilot
	coreConfig.Seal = f.Seal
	return coreConfig, opts
}

type raftClusterOpts struct {
	fileOpts
	DisableFollowerJoins           bool
	InmemCluster                   bool
	EnableResponseHeaderRaftNodeID bool
}

func modifyBackends(coreConfig *vault.CoreConfig, opts *vault.TestClusterOptions, rest any) (*vault.CoreConfig, *vault.TestClusterOptions) {
	switch ropts := rest.(type) {
	case *fileOpts:
		coreConfig, opts = ropts.reset(coreConfig, opts)
		teststorage.FileBackendSetup(coreConfig, opts)
	case *raftClusterOpts:
		coreConfig, opts = ropts.fileOpts.reset(coreConfig, opts)
		opts.InmemClusterLayers = ropts.InmemCluster
		coreConfig.EnableResponseHeaderRaftNodeID = ropts.EnableResponseHeaderRaftNodeID
		teststorage.RaftBackendSetup(coreConfig, opts)
		if ropts.DisableFollowerJoins {
			opts.SetupFunc = nil
		}
	default:
	}
	return coreConfig, opts
}
