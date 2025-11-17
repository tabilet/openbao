// Copyright (c) 2025 OpenBao a Series of LF Projects, LLC
// SPDX-License-Identifier: MPL-2.0

package physical

import (
	"context"
)

type Mountable interface {
	CreateMountView(ctx context.Context, prefix, uuid string) error
	DeleteMountView(ctx context.Context, prefix, uuid string) error
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
