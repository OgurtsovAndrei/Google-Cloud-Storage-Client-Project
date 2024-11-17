package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:          "gcsintro",
	SilenceUsage: true,
}

var uploadObj = &cobra.Command{
	Use:  "obj",
	RunE: runUploadObj,
}

var uploadObjArgs struct {
	bucket string
}

func init() {
	uploadObj.Flags().StringVarP(&uploadObjArgs.bucket, "bucket", "b", "", "destination bucket")
	rootCmd.AddCommand(uploadObj)
}

func runUploadObj(self *cobra.Command, args []string) (err error) {
	ctx := context.Background()

	if uploadObjArgs.bucket == "" {
		return errors.New("destination bucket must be specified")
	}

	c, err := NewGcsClient(ctx)
	if err != nil {
		return err
	}

	buf := makeRandBuf(4096)
	if err = c.UploadObject(ctx, uploadObjArgs.bucket, "x", buf); err != nil {
		return err
	}

	return nil
}

var uploadMultipartObj = &cobra.Command{
	Use:  "mobj",
	RunE: runUploadMultipartObj,
}

var uploadMultipartObjArgs struct {
	bucket string
}

func init() {
	uploadMultipartObj.Flags().StringVarP(&uploadMultipartObjArgs.bucket, "bucket", "b", "", "destination bucket")
	rootCmd.AddCommand(uploadMultipartObj)
}

func runUploadMultipartObj(self *cobra.Command, args []string) (err error) {
	ctx := context.Background()

	if uploadMultipartObjArgs.bucket == "" {
		return errors.New("destination bucket must be specified")
	}

	c, err := NewGcsClient(ctx)
	if err != nil {
		return err
	}

	uploadUrl, err := c.NewUploadSession(ctx, uploadMultipartObjArgs.bucket, "x")
	if err != nil {
		return err
	}

	const chunkSize = 256 * 1024
	off, buf := int64(0), makeRandBuf(2*chunkSize)

	if err = c.UploadObjectPart(ctx, uploadUrl, off, buf[:chunkSize], false); err != nil {
		return err
	}
	off += chunkSize
	buf = buf[chunkSize:]

	testOff, testLast, err := c.GetResumeOffset(ctx, uploadUrl)
	if err != nil {
		return err
	}
	fmt.Printf("GetResumeOffset() = %d, %t\n", testOff, testLast)

	if err = c.UploadObjectPart(ctx, uploadUrl, off, buf[:chunkSize], true); err != nil {
		return err
	}

	testOff, testLast, err = c.GetResumeOffset(ctx, uploadUrl)
	if err != nil {
		return err
	}
	fmt.Printf("GetResumeOffset() = %d, %t\n", testOff, testLast)

	return nil
}
