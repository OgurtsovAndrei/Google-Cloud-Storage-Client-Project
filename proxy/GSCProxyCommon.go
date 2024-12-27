package proxy

import (
	"awesomeProject/utils"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
)

const (
	MessageTypeInitConnection  = 0
	MessageTypeUploadPart      = 1
	MessageTypeGetResumeOffset = 2
	MessageTypeAbort           = 3
	MessageTypeHandshake       = 4
)

type RequestHeader struct {
	RequestUid  uint32
	RequestType uint32
}

type ResponseHeader struct {
	RequestUid uint32
	StatusCode int32
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

type HandshakeHeader struct {
	ClientIDLength uint32
}

type HandshakeRequest struct {
	Header          RequestHeader
	HandshakeHeader HandshakeHeader
	ClientID        string
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

func (req *HandshakeRequest) ToRequestMessage() RequestMessage {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.BigEndian, &req.HandshakeHeader); err != nil {
		panic("Failed to cast to bytes[] HandshakeHeader")
	}

	dataBytes := req.ClientID

	return RequestMessage{
		Header: RequestHeader{
			RequestUid:  req.Header.RequestUid,
			RequestType: MessageTypeHandshake,
		},
		SecondHeader: buf,
		Data:         strings.NewReader(dataBytes),
	}
}

func readRequestHeader(r io.Reader) (hdr RequestHeader, err error) {
	err = binary.Read(r, binary.BigEndian, &hdr)
	return hdr, err
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

func readHandshakeRequest(header RequestHeader, reader io.Reader) (*HandshakeRequest, error) {
	if header.RequestType != MessageTypeHandshake {
		return nil, errors.New("incorrect request type for HandshakeRequest")
	}

	var handshakeHeader HandshakeHeader
	if err := binary.Read(reader, binary.BigEndian, &handshakeHeader); err != nil {
		return nil, err
	}

	clientIDBytes := make([]byte, handshakeHeader.ClientIDLength)
	if _, err := io.ReadFull(reader, clientIDBytes); err != nil {
		return nil, err
	}

	return &HandshakeRequest{
		Header:          header,
		HandshakeHeader: handshakeHeader,
		ClientID:        string(clientIDBytes),
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
	case MessageTypeHandshake:
		return readHandshakeRequest(header, reader)
	default:
		return nil, &utils.Error{
			Code:  utils.ErrCodeUnknownRequestType,
			Msg:   fmt.Sprintf("unknown request type %d", header.RequestType),
			Cause: nil,
			Tags: []string{
				utils.TagIllegalArgument,
				utils.TagInternal,
			},
		}
	}
}

type requestReader struct {
	headerBuffer  *bytes.Buffer
	secondHeader  io.Reader
	data          io.Reader
	currentReader io.Reader
}

func NewRequestReader(req *RequestMessage) io.Reader {
	headerBuf := new(bytes.Buffer)
	_ = binary.Write(headerBuf, binary.BigEndian, req.Header)
	return &requestReader{
		headerBuffer:  headerBuf,
		secondHeader:  req.SecondHeader,
		data:          req.Data,
		currentReader: headerBuf,
	}
}

func (r *requestReader) Read(p []byte) (n int, err error) {
	for {
		if r.currentReader == nil {
			return n, io.EOF
		}
		m, err := r.currentReader.Read(p[n:])
		n += m
		if err == io.EOF {
			if r.currentReader == r.headerBuffer {
				r.currentReader = r.secondHeader
			} else if r.currentReader == r.secondHeader {
				r.currentReader = r.data
			} else {
				r.currentReader = nil
			}
			if n > 0 {
				return n, nil
			}
		} else if err != nil {
			return n, err
		} else {
			return n, nil
		}
	}
}

type responseReader struct {
	headerBuffer  *bytes.Buffer
	data          io.Reader
	currentReader io.Reader
}

func NewResponseReader(resp *ResponseMessage) io.Reader {
	//log.Println("NewResponseReader: Create Response Reader")
	headerBuf := new(bytes.Buffer)
	_ = binary.Write(headerBuf, binary.BigEndian, resp.Header)
	return &responseReader{
		headerBuffer:  headerBuf,
		data:          strings.NewReader(resp.Data),
		currentReader: headerBuf,
	}
}

func (r *responseReader) Read(p []byte) (n int, err error) {
	log.Println("Response Message: Starting Read")
	for {
		if r.currentReader == nil {
			return n, io.EOF
		}
		m, err := r.currentReader.Read(p[n:])
		//log.Printf("responseReader: Read %d bytes: %x", m, p[n:n+m])
		n += m
		if err == io.EOF {
			if r.currentReader == r.headerBuffer {
				r.currentReader = r.data
			} else {
				r.currentReader = nil
			}
			if n > 0 {
				return n, nil
			}
		} else if err != nil {
			return n, err
		} else {
			return n, nil
		}
	}
}

func SendSuccessResponse(conn io.Writer, requestUid uint32, message string) {
	fmt.Printf("Sending operation success response: %s\n", message)
	resp := BuildSucceedResponse(requestUid, message)

	_, err := io.Copy(conn, NewResponseReader(&resp))
	if err != nil {
		log.Printf("Sending operation success response failed: %s\n", err)
	}
}

func BuildSucceedResponse(requestUid uint32, message string) ResponseMessage {
	respHeader := ResponseHeader{
		RequestUid: requestUid,
		StatusCode: 0,
		DataLength: uint32(len(message)),
	}

	resp := ResponseMessage{
		Header: respHeader,
		Data:   message,
	}
	return resp
}

// ErrorToResponseMessage converts a utils.Error into a ResponseMessage
// If serialization fails, creates a simple ResponseMessage with StatusCode -2
func ErrorToResponseMessage(requestUid uint32, err *utils.Error) (*ResponseMessage, error) {
	// Attempt to serialize the error into JSON
	jsonError, serializationErr := err.ToJSON()
	if serializationErr != nil {
		// Fallback to a simple error response
		log.Printf("ErrorToResponseMessage: Failed to serialize error, using fallback. Error: %v", serializationErr)

		// Create a simple ResponseMessage
		fallbackMessage := &ResponseMessage{
			Header: ResponseHeader{
				RequestUid: requestUid,
				StatusCode: -2, // Indicates a fallback error response
				DataLength: uint32(len(serializationErr.Error())),
			},
			Data: serializationErr.Error(),
		}

		return fallbackMessage, serializationErr
	}

	// Create the standard ResponseMessage
	resp := &ResponseMessage{
		Header: ResponseHeader{
			RequestUid: requestUid,
			StatusCode: -1, // Indicates an error response
			DataLength: uint32(len(jsonError)),
		},
		Data: jsonError,
	}

	return resp, nil
}

func SendErrorResponse(conn io.Writer, requestUid uint32, err error) {
	if err == nil {
		SendSuccessResponse(conn, requestUid, "")
		return
	}

	resp := BuildErrorResponse(requestUid, err)

	_, writeErr := io.Copy(conn, NewResponseReader(resp))
	if writeErr != nil {
		log.Printf("SendErrorResponse Write Error: %s", writeErr)
	}
}

func BuildErrorResponse(requestUid uint32, err error) *ResponseMessage {
	var customErr *utils.Error
	if errors.As(err, &customErr) {
		resp, convertErr := ErrorToResponseMessage(requestUid, customErr)
		if convertErr != nil {
			log.Printf("Error converting custom error to ResponseMessage: %s", convertErr)
			return buildBasicErrorResponse(requestUid, err)
		}
		return resp
	}

	// Fallback: build a basic error response for non-utils.Error types
	return buildBasicErrorResponse(requestUid, err)
}

func buildBasicErrorResponse(requestUid uint32, err error) *ResponseMessage {
	msg := err.Error()
	return &ResponseMessage{
		Header: ResponseHeader{
			RequestUid: requestUid,
			StatusCode: -1,
			DataLength: uint32(len(msg)),
		},
		Data: msg,
	}
}

func ReadResponse(reader io.Reader) (*ResponseMessage, error) {
	log.Println("ReadResponse: Starting to read response header")

	// Read the response header
	var header ResponseHeader
	err := binary.Read(reader, binary.BigEndian, &header)
	if err != nil {
		if err == io.EOF {
			return nil, io.EOF // End of stream
		}
		return nil, fmt.Errorf("failed to read response header: %w", err)
	}
	log.Printf("ReadResponse: Successfully read response header: %+v", header)

	// Read the response data
	data := make([]byte, header.DataLength)
	_, err = io.ReadFull(reader, data)
	if err != nil {
		return nil, fmt.Errorf("failed to read response data: %w", err)
	}
	log.Printf("ReadResponse: Successfully read response data \\ header = %+v", header)

	// Construct the response message
	response := &ResponseMessage{
		Header: header,
		Data:   string(data),
	}

	return response, nil
}

// IsErr checks if the ResponseMessage represents an error.
func (resp *ResponseMessage) IsErr() bool {
	return resp.Header.StatusCode < 0
}

// AsErr deserializes the ResponseMessage into a utils.Error if it represents an error.
func (resp *ResponseMessage) AsErr() (*utils.Error, *utils.Error) {
	if !resp.IsErr() {
		return nil, &utils.Error{
			Code:  utils.InvalidArgumentError,
			Msg:   fmt.Sprintf("invalid response message cast: %s", resp.Data),
			Cause: fmt.Errorf("invalid response message cast"),
			Tags:  []string{utils.TagInternal},
		}
	}

	customErr, err := utils.FromJSON(resp.Data)
	if err != nil {
		return nil, &utils.Error{
			Code:  utils.ErrCastFailed,
			Msg:   fmt.Sprintf("Server response message cast failed, msg: %s", resp.Data),
			Cause: nil,
			Tags:  nil,
		}
	}

	return customErr, nil
}
