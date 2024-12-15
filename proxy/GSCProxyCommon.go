package proxy

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
)

const (
	MessageTypeInitConnection  = 0
	MessageTypeUploadPart      = 1
	MessageTypeGetResumeOffset = 2
	MessageTypeAbort           = 3
)

type RequestHeader struct {
	RequestUid  uint32
	RequestType uint32
}

type ResponseHeader struct {
	RequestUid uint32
	StatusCode uint32
	DataLength uint32
}

type RequestMessage struct {
	Header       RequestHeader
	SecondHeader io.Reader
	Data         io.Reader
}

type ResponseMessage struct {
	Header ResponseHeader
	Data   string
}

type InitUploadSessionHeader struct {
	BucketNameLength uint32
	ObjectNameLength uint32
}

type GetResumeOffsetHeader struct {
	BucketNameLength uint32
	ObjectNameLength uint32
}

type WriteAtHeader struct {
	BucketNameLength uint32
	ObjectNameLength uint32
	ChunkBegin       int64
	ChunkEnd         int64
	Off              int64
	Size             int64
	IsLast           byte
}

type AbortHeader struct {
	BucketNameLength uint32
	ObjectNameLength uint32
}

type InitUploadSessionRequest struct {
	Header                  RequestHeader
	InitUploadSessionHeader InitUploadSessionHeader
	Bucket                  string
	Object                  string
}

type AbortRequest struct {
	Header      RequestHeader
	AbortHeader AbortHeader
	Bucket      string
	Object      string
}

type GetResumeOffsetRequest struct {
	Header                RequestHeader
	GetResumeOffsetHeader GetResumeOffsetHeader
	Bucket                string
	Object                string
}

type WriteAtRequest struct {
	Header        RequestHeader
	WriteAtHeader WriteAtHeader
	Bucket        string
	Object        string
	Data          io.Reader
}

func encodeString(str string) []byte {
	length := uint32(len(str))
	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.BigEndian, length)
	buf.WriteString(str)
	return buf.Bytes()
}

func (req *WriteAtRequest) ToRequestMessage() RequestMessage {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.BigEndian, &req.WriteAtHeader); err != nil {
		panic("Failed to cast to bytes[] WriteAtHeader")
	}

	dataBytes := req.Bucket + req.Object

	return RequestMessage{
		Header: RequestHeader{
			RequestUid:  req.Header.RequestUid,
			RequestType: MessageTypeUploadPart,
		},
		SecondHeader: buf,
		Data:         io.MultiReader(strings.NewReader(dataBytes), req.Data),
	}
}

func (req *InitUploadSessionRequest) ToRequestMessage() RequestMessage {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.BigEndian, &req.InitUploadSessionHeader); err != nil {
		panic("Failed to cast to bytes[] InitUploadSessionHeader")
	}

	dataBytes := req.Bucket + req.Object
	return RequestMessage{
		Header: RequestHeader{
			RequestUid:  req.Header.RequestUid,
			RequestType: MessageTypeInitConnection,
		},
		SecondHeader: buf,
		Data:         strings.NewReader(dataBytes),
	}
}

func (req *GetResumeOffsetRequest) ToRequestMessage() RequestMessage {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, &req.GetResumeOffsetHeader); err != nil {
		panic("Failed to cast to bytes[] GetResumeOffsetHeader")
	}

	dataBytes := req.Bucket + req.Object
	return RequestMessage{
		Header: RequestHeader{
			RequestUid:  req.Header.RequestUid,
			RequestType: MessageTypeGetResumeOffset,
		},
		SecondHeader: buf,
		Data:         strings.NewReader(dataBytes),
	}
}

func (req *AbortRequest) ToRequestMessage() RequestMessage {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, &req.AbortHeader); err != nil {
		panic("Failed to cast to bytes[] AbortHeader")
	}

	dataBytes := req.Bucket + req.Object
	return RequestMessage{
		Header: RequestHeader{
			RequestUid:  req.Header.RequestUid,
			RequestType: MessageTypeAbort,
		},
		SecondHeader: buf,
		Data:         strings.NewReader(dataBytes),
	}
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

func readRequestHeader(reader io.Reader) (RequestHeader, error) {
	var header RequestHeader
	err := binary.Read(reader, binary.BigEndian, &header)
	if err != nil {
		return header, err
	}
	return header, nil
}

func readInitUploadSessionRequest(header RequestHeader, reader io.Reader) (*InitUploadSessionRequest, error) {
	if header.RequestType != MessageTypeInitConnection {
		return nil, errors.New("incorrect request type for InitUploadSessionRequest")
	}

	var initHeader InitUploadSessionHeader
	if err := binary.Read(reader, binary.BigEndian, &initHeader); err != nil {
		return nil, err
	}

	bucketBytes := make([]byte, initHeader.BucketNameLength)
	if _, err := io.ReadFull(reader, bucketBytes); err != nil {
		return nil, err
	}

	objectBytes := make([]byte, initHeader.ObjectNameLength)
	if _, err := io.ReadFull(reader, objectBytes); err != nil {
		return nil, err
	}

	return &InitUploadSessionRequest{
		Header:                  header,
		InitUploadSessionHeader: initHeader,
		Bucket:                  string(bucketBytes),
		Object:                  string(objectBytes),
	}, nil
}

func readGetResumeOffsetRequest(header RequestHeader, reader io.Reader) (*GetResumeOffsetRequest, error) {
	if header.RequestType != MessageTypeGetResumeOffset {
		return nil, errors.New("incorrect request type for GetResumeOffsetRequest")
	}

	var getHeader GetResumeOffsetHeader
	if err := binary.Read(reader, binary.BigEndian, &getHeader); err != nil {
		return nil, err
	}

	bucketBytes := make([]byte, getHeader.BucketNameLength)
	if _, err := io.ReadFull(reader, bucketBytes); err != nil {
		return nil, err
	}

	objectBytes := make([]byte, getHeader.ObjectNameLength)
	if _, err := io.ReadFull(reader, objectBytes); err != nil {
		return nil, err
	}

	return &GetResumeOffsetRequest{
		Header:                header,
		GetResumeOffsetHeader: getHeader,
		Bucket:                string(bucketBytes),
		Object:                string(objectBytes),
	}, nil
}

func readWriteAtRequest(header RequestHeader, reader io.Reader) (*WriteAtRequest, error) {
	if header.RequestType != MessageTypeUploadPart {
		return nil, errors.New("incorrect request type for WriteAtRequest")
	}

	var writeHeader WriteAtHeader
	if err := binary.Read(reader, binary.BigEndian, &writeHeader); err != nil {
		return nil, err
	}

	bucketBytes := make([]byte, writeHeader.BucketNameLength)
	if _, err := io.ReadFull(reader, bucketBytes); err != nil {
		return nil, err
	}

	objectBytes := make([]byte, writeHeader.ObjectNameLength)
	if _, err := io.ReadFull(reader, objectBytes); err != nil {
		return nil, err
	}

	return &WriteAtRequest{
		Header:        header,
		WriteAtHeader: writeHeader,
		Bucket:        string(bucketBytes),
		Object:        string(objectBytes),
		Data:          io.LimitReader(reader, writeHeader.Size), // Remaining Data in the reader is the payload
	}, nil
}

func readAbortRequest(header RequestHeader, reader io.Reader) (*AbortRequest, error) {
	if header.RequestType != MessageTypeAbort {
		return nil, errors.New("incorrect request type for AbortRequest")
	}

	var abortHeader AbortHeader
	if err := binary.Read(reader, binary.BigEndian, &abortHeader); err != nil {
		return nil, err
	}

	bucketBytes := make([]byte, abortHeader.BucketNameLength)
	if _, err := io.ReadFull(reader, bucketBytes); err != nil {
		return nil, err
	}

	objectBytes := make([]byte, abortHeader.ObjectNameLength)
	if _, err := io.ReadFull(reader, objectBytes); err != nil {
		return nil, err
	}

	return &AbortRequest{
		Header:      header,
		AbortHeader: abortHeader,
		Bucket:      string(bucketBytes),
		Object:      string(objectBytes),
	}, nil
}

func ReadRequest(reader io.Reader) (interface{}, error) {
	header, err := readRequestHeader(reader)
	fmt.Printf("Read 8 bytes of request header: %x\n", header)
	if err != nil {
		return nil, err
	}

	switch header.RequestType {
	case MessageTypeInitConnection:
		return readInitUploadSessionRequest(header, reader)
	case MessageTypeGetResumeOffset:
		return readGetResumeOffsetRequest(header, reader)
	case MessageTypeUploadPart:
		return readWriteAtRequest(header, reader)
	case MessageTypeAbort:
		return readAbortRequest(header, reader)
	default:
		return nil, errors.New("unknown request type")
	}
}
