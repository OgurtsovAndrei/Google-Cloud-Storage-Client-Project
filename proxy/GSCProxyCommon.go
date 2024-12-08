package proxy

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

type ResponseHeader struct {
	SequenceNumber uint32
	StatusCode     uint32
	MessageLength  uint32
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

func RequestTypeToString(requestType uint32) string {
	switch requestType {
	case MessageTypeInitConnection:
		return "InitConnection"
	case MessageTypeUploadPart:
		return "UploadPart"
	case MessageTypeGetResumeOffset:
		return "GetResumeOffset"
	case MessageTypeAbort:
		return "Abort"
	default:
		return "Unknown"
	}
}
