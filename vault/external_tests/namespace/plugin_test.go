package namespace

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/openbao/openbao/api/v2"
)

func TestPluginRoot(t *testing.T) {
	t.Parallel()
	for i, b := range []any{new(raftClusterOpts), new(fileOpts), nil} {
		t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
			client, plugin, closer := testPluginServer(t, b)
			defer closer()

			ctx := context.Background()
			sys := client.Sys()

			command := plugin.Name
			err := sys.RegisterPluginWithContext(ctx, &api.RegisterPluginInput{
				Type:    api.PluginType(plugin.Typ),
				Name:    command,
				SHA256:  plugin.Sha256,
				Command: command,
				Version: plugin.Version,
			})
			if err != nil {
				t.Fatal(err)
			}

			path := "graph"
			err = mountPlugin(ctx, sys, command, path, []string{path + "/", "cubbyhole/", "identity/", "sys/", "secret/"})
			if err != nil {
				t.Fatal(err)
			}

			err = unmountPlugin(ctx, sys, path, []string{"cubbyhole/", "identity/", "sys/", "secret/"})
			if err != nil {
				t.Fatal(err)
			}

			err = deregisterPlugin(ctx, sys, command)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPluginNamespace(t *testing.T) {
	t.Parallel()
	for i, b := range []any{new(raftClusterOpts), new(fileOpts), nil} {
		t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
			client, plugin, closer := testPluginServer(t, b)
			defer closer()

			ctx := context.Background()
			rootNS := "pname"
			clone, err := cloneClient(ctx, client, rootNS)
			if err != nil {
				t.Fatal(err)
			}

			sys := clone.Sys()

			command := plugin.Name
			err = sys.RegisterPluginWithContext(ctx, &api.RegisterPluginInput{
				Type:    api.PluginType(plugin.Typ),
				Name:    command,
				SHA256:  plugin.Sha256,
				Command: command,
				Version: plugin.Version,
			})
			if err != nil {
				t.Fatal(err)
			}

			path := "graph"
			err = mountPlugin(ctx, sys, command, path, []string{path + "/", "cubbyhole/", "identity/", "sys/", "secret/"})
			if err != nil {
				t.Fatal(err)
			}

			err = unmountPlugin(ctx, sys, path, []string{"cubbyhole/", "identity/", "sys/", "secret/"})
			if err != nil {
				t.Fatal(err)
			}

			err = deregisterPlugin(ctx, sys, command)
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

func TestPluginMix(t *testing.T) {
	t.Parallel()
	for i, b := range []any{new(raftClusterOpts), new(fileOpts), nil} {
		t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
			client, plugin, closer := testPluginServer(t, b)
			defer closer()

			ctx := context.Background()
			rootNS := "pname"
			clone, err := cloneClient(ctx, client, rootNS)
			if err != nil {
				t.Fatal(err)
			}

			sys1 := client.Sys()
			sys2 := clone.Sys()

			command := plugin.Name
			err = sys1.RegisterPluginWithContext(ctx, &api.RegisterPluginInput{
				Type:    api.PluginType(plugin.Typ),
				Name:    command,
				SHA256:  plugin.Sha256,
				Command: command,
				Version: plugin.Version,
			})
			if err != nil {
				t.Fatal(err)
			}
			err = sys2.RegisterPluginWithContext(ctx, &api.RegisterPluginInput{
				Type:    api.PluginType(plugin.Typ),
				Name:    command,
				SHA256:  plugin.Sha256,
				Command: command,
				Version: plugin.Version,
			})
			if err != nil {
				t.Fatal(err)
			}

			path := "graph"
			err = mountPlugin(ctx, sys1, command, path, []string{path + "/", "cubbyhole/", "identity/", "sys/", "secret/"})
			if err != nil {
				t.Fatal(err)
			}
			err = mountPlugin(ctx, sys2, command, path, []string{path + "/", "cubbyhole/", "identity/", "sys/", "secret/"})
			if err != nil {
				t.Fatal(err)
			}

			err = unmountPlugin(ctx, sys1, path, []string{"cubbyhole/", "identity/", "sys/", "secret/"})
			if err != nil {
				t.Fatal(err)
			}
			err = unmountPlugin(ctx, sys2, path, []string{"cubbyhole/", "identity/", "sys/", "secret/"})
			if err != nil {
				t.Fatal(err)
			}

			err = deregisterPlugin(ctx, sys1, command)
			if err != nil {
				t.Fatal(err)
			}
			err = deregisterPlugin(ctx, sys2, command)
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

func mountPlugin(ctx context.Context, sys *api.Sys, command, path string, ref []string) error {
	err := sys.MountWithContext(ctx, path, &api.MountInput{
		Type: command,
	})
	if err != nil {
		return err
	}

	mountsRspn, err := sys.ListMountsWithContext(ctx)
	if err != nil {
		return err
	}
	for k, rspn := range mountsRspn {
		if !slices.Contains(ref, k) {
			return fmt.Errorf("mount response: %s => %+v", k, rspn)
		}
	}
	return nil
}

func unmountPlugin(ctx context.Context, sys *api.Sys, path string, ref []string) error {
	mountsRspn, err := sys.ListMountsWithContext(ctx)
	if err != nil {
		return err
	}
	for k, rspn := range mountsRspn {
		if !slices.Contains(append(ref, path+"/"), k) {
			return fmt.Errorf("mount response1: %s => %+v", k, rspn)
		}
	}

	err = sys.UnmountWithContext(ctx, path)
	if err != nil {
		return err
	}
	mountsRspn, err = sys.ListMountsWithContext(ctx)
	if err != nil {
		return err
	}
	for k, rspn := range mountsRspn {
		if !slices.Contains(ref, k) {
			return fmt.Errorf("mount response2: %s => %+v", k, rspn)
		}
	}
	return nil
}

func deregisterPlugin(ctx context.Context, sys *api.Sys, command string) error {
	err := sys.DeregisterPluginWithContext(ctx, &api.DeregisterPluginInput{
		Type: api.PluginTypeSecrets,
		Name: command,
	})
	if err != nil {
		return err
	}
	rspn, err := sys.ListPluginsWithContext(ctx, &api.ListPluginsInput{
		Type: api.PluginTypeSecrets,
	})
	if err != nil {
		return err
	}
	if slices.Contains(rspn.PluginsByType[api.PluginTypeSecrets], command) {
		return fmt.Errorf("plugin response: %+v", rspn.PluginsByType[api.PluginTypeSecrets])
	}
	return nil
}
