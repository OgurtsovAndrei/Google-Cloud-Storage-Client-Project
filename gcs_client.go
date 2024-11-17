package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"cloud.google.com/go/storage"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	raw "google.golang.org/api/storage/v1"
	htransport "google.golang.org/api/transport/http"
)

type GcsClient struct {
	h http.Client
}

func NewGcsClient(ctx context.Context) (c *GcsClient, err error) {
	creds, err := google.FindDefaultCredentials(ctx, storage.ScopeFullControl)
	if err != nil {
		return nil, fmt.Errorf("failed to load the credentials: %w", err)
	}

	ts := oauth2.ReuseTokenSourceWithExpiry(nil, creds.TokenSource, time.Minute)
	tr, err := htransport.NewTransport(ctx, http.DefaultTransport,
		option.WithTokenSource(ts),
		option.WithTelemetryDisabled(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to build the transport: %w", err)
	}

	c = &GcsClient{
		h: http.Client{Transport: tr},
	}
	return c, nil
}

func (c *GcsClient) UploadObject(ctx context.Context, bucket, name string, data []byte) (err error) {
	req, err := http.NewRequestWithContext(ctx,
		http.MethodPut, c.objectUrl(bucket, name),
		bytes.NewReader(data))
	if err != nil {
		return err
	}

	resp, err := c.h.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("upload failed with status %q", resp.Status)
	}

	return nil
}

func (c *GcsClient) NewUploadSession(ctx context.Context, bucket, name string) (uploadUrl string, err error) {
	args := saveJson(raw.Object{
		Bucket: bucket,
		Name:   name,
	})

	req, err := http.NewRequestWithContext(ctx,
		http.MethodPost, c.newResumableUploadUrl(bucket),
		bytes.NewReader(args))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.h.Do(req)
	if err != nil {
		return "", nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("starting an upload failed with status %q", resp.Status)
	}

	uploadUrl = resp.Header.Get("Location")
	if uploadUrl == "" {
		return "", fmt.Errorf("no location header in the response")
	}

	return uploadUrl, nil
}

func (c *GcsClient) UploadObjectPart(ctx context.Context, uploadUrl string, off int64, data []byte, last bool) (err error) {
	req, err := http.NewRequestWithContext(ctx,
		http.MethodPut, uploadUrl,
		bytes.NewReader(data))
	if err != nil {
		return err
	}

	var contentRange string
	if last {
		if len(data) == 0 {
			contentRange = fmt.Sprintf("bytes */%d", off)
		} else {
			begin, end := off, off+int64(len(data))
			contentRange = fmt.Sprintf("bytes %d-%d/%d", begin, end-1, end)
		}
	} else {
		if len(data)%googleapi.MinUploadChunkSize != 0 {
			return fmt.Errorf("unaligned chunk, size=%d", len(data))
		}
		if len(data) == 0 {
			return fmt.Errorf("only the last chunk may be empty")
		}

		begin, end := off, off+int64(len(data))
		contentRange = fmt.Sprintf("bytes %d-%d/*", begin, end-1)
	}

	req.Header.Set("Content-Range", contentRange)
	req.ContentLength = int64(len(data))
	req.Header.Set("X-GUploader-No-308", "yes")

	resp, err := c.h.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	_, _, err = c.parseOffsetResponse(resp)
	return err
}

func (c *GcsClient) GetResumeOffset(ctx context.Context, uploadUrl string) (off int64, complete bool, err error) {
	req, err := http.NewRequestWithContext(ctx,
		http.MethodPut, uploadUrl,
		nil)
	if err != nil {
		return 0, false, err
	}

	req.Header.Set("Content-Range", "bytes */*")
	req.Header.Set("X-GUploader-No-308", "yes")

	resp, err := c.h.Do(req)
	if err != nil {
		return 0, false, nil
	}
	defer resp.Body.Close()

	return c.parseOffsetResponse(resp)
}

func (c *GcsClient) parseOffsetResponse(resp *http.Response) (off int64, complete bool, err error) {
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return 0, false, fmt.Errorf("chunk upload failed with status %q", resp.Status)
	}

	if resp.Header.Get("X-HTTP-Status-Code-Override") != "308" {
		// An object upload was successfully completed. This response has
		// no Range: header, but the body is a JSON describing the uploaded
		// object.

		var obj raw.Object
		if err = json.NewDecoder(resp.Body).Decode(&obj); err != nil {
			return 0, false, fmt.Errorf("GCS response is not a valid JSON")
		}
		return int64(obj.Size), true, nil
	}

	r := resp.Header.Get("Range")
	if r == "" {
		// This may happen after uploading 0 bytes.
		return 0, false, nil
	}

	if _, err = fmt.Sscanf(r, "bytes=0-%d", &off); err != nil {
		return 0, false, fmt.Errorf("GCS sent a malformed Range: as a reply: %q", r)
	}

	// Range: specifies the bytes range as [0, x] instead of [0, x).
	off += 1

	return off, false, nil
}

func (c *GcsClient) CancelUpload(ctx context.Context, uploadUrl string) (err error) {
	req, err := http.NewRequestWithContext(ctx,
		http.MethodDelete, uploadUrl,
		nil)
	if err != nil {
		return err
	}

	resp, err := c.h.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	// for some reason GCS may reply 499 to this request
	if resp.StatusCode != http.StatusOK && resp.StatusCode != 499 {
		return fmt.Errorf("upload failed with status %q", resp.Status)
	}

	return nil
}

func (c *GcsClient) objectUrl(bucket, name string) string {
	return fmt.Sprintf("https://%s.storage.googleapis.com/%s", bucket, name)
}

func (c *GcsClient) newResumableUploadUrl(bucket string) string {
	return fmt.Sprintf("https://storage.googleapis.com/upload/storage/v1/b/%s/o?uploadType=resumable", bucket)
}
