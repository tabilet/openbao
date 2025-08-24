package mountable

import (
	"context"

	"github.com/openbao/openbao/helper/namespace"
	"github.com/openbao/openbao/sdk/v2/physical"
)

type Mountable interface {
	CreateIfNotExists(ctx context.Context, ns *namespace.Namespace) error
	DropIfExists(ctx context.Context, ns *namespace.Namespace) error
}

// PhysicalToMountable verifies if a physical backend is also mountable
func PhysicalToMountable(pb physical.Backend) (Mountable, bool) {
	x := pb
	if t, ok := physical.IsErrorInject(x); ok {
		x = t
	}
	physical, ok := x.(Mountable)
	return physical, ok
}
