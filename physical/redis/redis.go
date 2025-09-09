package redis

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	metrics "github.com/armon/go-metrics"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-secure-stdlib/strutil"
	"github.com/mediocregopher/radix/v4"
	"github.com/openbao/openbao/api/v2"
	"github.com/openbao/openbao/helper/namespace"
	"github.com/openbao/openbao/sdk/v2/physical"
)

// Verify RedisBackend satisfies the correct interfaces
var (
	delimeter                    = ":"
	_         physical.Backend   = (*RedisBackend)(nil)
	_         physical.Mountable = (*RedisBackend)(nil)
)

// RedisBackend is a physical backend that stores data
// within Redis database.
type RedisBackend struct {
	client     radix.Client
	logger     hclog.Logger
	permitPool *physical.PermitPool
	conf       map[string]string

	updateLock sync.Mutex
	mountLock  sync.RWMutex
}

func NewRedisPhysicalBackend(conf map[string]string, logger hclog.Logger) (physical.Backend, error) {
	return NewRedisBackend(conf, logger)
}

// NewRedisBackend constructs a Redis backend using the given API client and
// server address and credential for accessing redis database.
func NewRedisBackend(conf map[string]string, logger hclog.Logger) (*RedisBackend, error) {
	var user, pass, network, addr, sizeStr string
	var size int

	user = api.ReadBaoVariable("BAO_REDIS_USER")
	pass = api.ReadBaoVariable("BAO_REDIS_PASS")
	network = api.ReadBaoVariable("BAO_REDIS_NETWORK")
	addr = api.ReadBaoVariable("BAO_REDIS_ADDR")
	sizeStr = api.ReadBaoVariable("BAO_REDIS_SIZE")

	if user == "" {
		user = conf["redis_user"]
	}
	if pass == "" {
		pass = conf["redis_pass"]
	}
	if network == "" {
		network = conf["redis_network"]
		if network == "" {
			network = "tcp"
		}
	}
	if addr == "" {
		addr = conf["redis_addr"]
	}
	if sizeStr == "" {
		sizeStr = conf["redis_size"]
	}
	if sizeStr == "" {
		size = 20
	} else {
		var err error
		size, err = strconv.Atoi(sizeStr)
		if err != nil {
			return nil, fmt.Errorf("failed to convert redis_size to int: %w", err)
		}
	}

	cfg := radix.PoolConfig{
		Dialer: radix.Dialer{
			AuthUser: user,
			AuthPass: pass,
		},
		Size: size,
	}

	ctx := context.Background()
	redis, err := cfg.New(ctx, network, addr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}
	/*
		tname, err := tableFromContext(ctx)
		if err != nil {
			return nil, err
		}
		err = redis.Do(ctx, radix.Cmd(nil, "SET", tname+delimeter, ""))
	*/

	if logger == nil {
		logger = hclog.Default()
	}

	// Setup the backend.
	m := &RedisBackend{
		client:     redis,
		logger:     logger,
		permitPool: physical.NewPermitPool(size),
		conf:       conf,
	}

	return m, nil
}

func quote(s string) string {
	return strings.ReplaceAll(s, `;`, ``)
}

func tworeplace(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "-", ""), "/", delimeter)
}

// tableFromNamespace returns the table name for the current namespace.
func tableFromNamespace(ns string) (string, error) {
	path := namespace.RootNamespaceID
	if ns != "" {
		if strings.Contains(ns, delimeter) {
			return "", fmt.Errorf("invalid namespace path %s", ns)
		}
		path += delimeter + strings.Trim(ns, "/")
		path = tworeplace(path)
	}

	return quote(path), nil
}

// tableFromContext returns table name without database name, from context.
func tableFromContext(ctx context.Context) (string, error) {
	ns, err := namespace.FromContext(ctx)
	if err != nil && err == namespace.ErrNoNamespace {
		return namespace.RootNamespaceID, nil
	} else if err != nil {
		return "", fmt.Errorf("namespace in ctx error: %w", err)
	}

	return tableFromNamespace(ns.Path)
}

func (m *RedisBackend) existing(ctx context.Context, statement string) (bool, error) {
	var exists int
	err := m.client.Do(ctx, radix.Cmd(&exists, "EXISTS", statement))
	return exists > 0, err
}

// CreateIfNotExists creates the table if it does not exist.
func (m *RedisBackend) CreateIfNotExists(ctx context.Context, path string) error {
	defer metrics.MeasureSince([]string{"redis", "create if not exists"}, time.Now())

	parent, err := tableFromContext(ctx)
	if err != nil {
		return err
	}
	parentExist, err := m.existing(ctx, parent)
	if err != nil {
		m.logger.Error("check parent table exists", "parent", parent, "error", err)
		return err
	} else if !parentExist {
		m.logger.Error("parent namespace not found", "parent", parent)
		return fmt.Errorf("parent namespace not found")
	}

	tname, err := tableFromNamespace(path)
	if err != nil {
		m.logger.Error("get table name", "path", path, "error", err)
		return err
	}

	m.logger.Debug("redis table is ready to be created", "table", tname)
	return nil
}

// DropIfExists drop the table if it exists.
func (m *RedisBackend) DropIfExists(ctx context.Context, path string) error {
	defer metrics.MeasureSince([]string{"redis", "drop if exists"}, time.Now())

	tname, err := tableFromNamespace(path)
	if err != nil {
		m.logger.Error("get table name", "path", path, "error", err)
		return err
	}

	err = m.client.Do(ctx, radix.Cmd(nil, "DEL", tname))
	if err != nil {
		m.logger.Error("drop table", "table", tname, "error", err)
		return fmt.Errorf("drop table %s: %w", tname, err)
	}

	m.logger.Debug("redis table dropped", "table", tname)
	return err
}

// Get is used to fetch an entry.
func (m *RedisBackend) Get(ctx context.Context, key string) (*physical.Entry, error) {
	defer metrics.MeasureSince([]string{"redis", "get"}, time.Now())

	tname, err := tableFromContext(ctx)
	if err != nil {
		m.logger.Error("get table name", "context", ctx, "error", err)
		return nil, err
	}

	var value []byte
	err = m.client.Do(ctx, radix.Cmd(&value, "HGET", tname, key))
	if err != nil {
		m.logger.Error("get", "table", tname, "key", key, "error", err)
		return nil, fmt.Errorf("failed to get %w", err)
	}

	if value == nil {
		return nil, nil
	}

	m.logger.Debug("get", "table", tname, "key", key)
	return &physical.Entry{
		Key:   key,
		Value: value,
	}, nil
}

// Put is used to insert or update an entry.
func (m *RedisBackend) Put(ctx context.Context, entry *physical.Entry) error {
	defer metrics.MeasureSince([]string{"redis", "put"}, time.Now())

	tname, err := tableFromContext(ctx)
	if err != nil {
		m.logger.Error("get table name", "context", ctx, "error", err)
		return err
	}
	key := entry.Key
	err = m.client.Do(ctx, radix.Cmd(nil, "HSET", tname, key, string(entry.Value)))
	if err != nil {
		m.logger.Error("put", "table", tname, "key", key, "error", err)
		return fmt.Errorf("failed to put %w", err)
	}

	m.logger.Debug("put", "table", tname, "key", key)
	return nil
}

// Delete is used to permanently delete an entry
func (m *RedisBackend) Delete(ctx context.Context, key string) error {
	defer metrics.MeasureSince([]string{"redis", "delete"}, time.Now())

	tname, err := tableFromContext(ctx)
	if err != nil {
		m.logger.Error("get table name", "context", ctx, "error", err)
		return err
	}

	err = m.client.Do(ctx, radix.Cmd(nil, "HDEL", tname, key))
	if err != nil {
		m.logger.Error("delete", "table", tname, "key", key, "error", err)
		return fmt.Errorf("failed to delete %w", err)
	}

	m.logger.Debug("delete", "table", tname, "key", key)

	return nil
}

// getNames returns table name and sorted hash keys of a table
func (m *RedisBackend) getNames(ctx context.Context, prefix string) (string, []string, error) {
	defer metrics.MeasureSince([]string{"redis", "get_names"}, time.Now())

	tname, err := tableFromContext(ctx)
	if err != nil {
		m.logger.Error("get table name", "context", ctx, "error", err)
		return "", nil, err
	}

	var arr, names []string
	err = m.client.Do(ctx, radix.Cmd(&arr, "HKEYS", tname))
	if err != nil {
		m.logger.Error("list page", "table", tname, "error", err)
		return "", nil, fmt.Errorf("failed to list keys: %w", err)
	}

	for _, key := range arr {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		name := strings.TrimPrefix(key, prefix)
		if i := strings.Index(name, "/"); i == -1 {
			// Add objects only from the current 'folder'
			names = append(names, name)
		} else if i != -1 {
			// Add truncated 'folder' paths
			names = strutil.AppendIfMissing(names, string(name[:i+1]))
		}
	}

	if len(names) > 0 {
		sort.Strings(names)
	}
	return tname, names, nil
}

// List is used to list the data of table under a given prefix
func (m *RedisBackend) List(ctx context.Context, prefix string) ([]string, error) {
	defer metrics.MeasureSince([]string{"redis", "list"}, time.Now())

	tname, names, err := m.getNames(ctx, prefix)
	if err != nil {
		return nil, err
	}

	m.logger.Debug("list", "table", tname, "prefix", prefix, "keys", len(names))
	return names, nil
}

func (m *RedisBackend) ListPage(ctx context.Context, prefix string, after string, limit int) ([]string, error) {
	defer metrics.MeasureSince([]string{"redis", "list_page"}, time.Now())

	tname, names, err := m.getNames(ctx, prefix)
	if err != nil {
		return nil, err
	}

	if after != "" {
		idx := sort.SearchStrings(names, after)
		if idx < len(names) && names[idx] == after {
			idx += 1
		}
		names = names[idx:]
	}

	if limit > 0 {
		if limit > len(names) {
			limit = len(names)
		}
		names = names[0:limit]
	}

	m.logger.Debug("list page", "table", tname, "prefix", prefix, "after", after, "limit", limit, "keys", len(names))

	return names, nil
}
