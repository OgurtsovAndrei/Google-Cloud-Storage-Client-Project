package writers

const (
	MessageTypeInitConnection  = 0
	MessageTypeUploadPart      = 1
	MessageTypeGetResumeOffset = 2
	MessageTypeAbort           = 3
)

type RequestHeader struct {
	SequenceNumber uint32
	RequestType    uint32
	RequestSize    uint32
}

type InitConnectionRequestHeader struct {
	BucketNameLength uint32
	ObjectNameLength uint32
}

type WriteAtRequestHeader struct {
	ChunkBegin int64
	ChunkEnd   int64
	IsLast     byte
}
