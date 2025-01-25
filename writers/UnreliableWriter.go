package writers

import (
	"awesomeProject/utils"
	"context"
)

// UnreliableWriter Not thread safe, so all methods calls have to be synchronized by caller
type UnreliableWriter interface {
	WriteAt(ctx context.Context, chunkBegin, chunkEnd int64, reader *ScatterGatherBuffer, isLast bool) (int64, *utils.Error)
	GetResumeOffset(ctx context.Context) (int64, *utils.Error)
	Abort(ctx context.Context)
}
