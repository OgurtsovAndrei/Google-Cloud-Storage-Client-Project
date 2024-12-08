package writers

import (
	"awesomeProject/utils"
	"context"
)

type Error struct {
	Code  string
	Msg   string
	Cause error
}

// UnreliableWriter Not thread safe, so all methods calls have to be synchronized by caller
type UnreliableWriter interface {
	WriteAt(ctx context.Context, chunkBegin, chunkEnd int64, reader *utils.ScatterGatherBuffer, isLast bool) (int64, error)
	GetResumeOffset(ctx context.Context) (int64, error)
	Abort(ctx context.Context)
}
