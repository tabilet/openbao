package physical

import (
	"context"
)

type Mountable interface {
	CreateIfNotExists(ctx context.Context, path string) error
	DropIfExists(ctx context.Context, path string) error
}

// PhysicalToMountable verifies if a physical backend is also mountable
func PhysicalToMountable(pb Backend) (Mountable, bool) {
	x := pb
	if t, ok := x.(*errorInjector); ok {
		x = t
	}
	physical, ok := x.(Mountable)
	return physical, ok
}
