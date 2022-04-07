// Code generated by protoc-gen-go.
// source: github.com/go-micro/plugins/client/http/proto/test.proto
// DO NOT EDIT!

/*
Package test is a generated protocol buffer package.

It is generated from these files:
	github.com/go-micro/plugins/client/http/proto/test.proto

It has these top-level messages:
	Message
*/
package test

import (
	fmt "fmt"

	proto "github.com/golang/protobuf/proto"

	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type Message struct {
	Seq  int64  `protobuf:"varint,1,opt,name=seq" json:"seq,omitempty"`
	Data string `protobuf:"bytes,2,opt,name=data" json:"data,omitempty"`
}

func (m *Message) Reset()                    { *m = Message{} }
func (m *Message) String() string            { return proto.CompactTextString(m) }
func (*Message) ProtoMessage()               {}
func (*Message) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

func init() {
	proto.RegisterType((*Message)(nil), "test.Message")
}

func init() {
	proto.RegisterFile("github.com/go-micro/plugins/client/http/proto/test.proto", fileDescriptor0)
}

var fileDescriptor0 = []byte{
	// 131 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x09, 0x6e, 0x88, 0x02, 0xff, 0x1c, 0xcb, 0xb1, 0x0e, 0x82, 0x30,
	0x10, 0x06, 0xe0, 0x54, 0x88, 0xc6, 0x4e, 0xa6, 0x13, 0x23, 0x71, 0x62, 0x91, 0x1b, 0x5c, 0x7c,
	0x09, 0x17, 0xde, 0xa0, 0xd4, 0x4b, 0x69, 0x02, 0x5c, 0xe5, 0x7e, 0xde, 0x9f, 0xd0, 0xed, 0x5b,
	0x3e, 0xfb, 0x89, 0x09, 0xd3, 0x3e, 0xf6, 0x41, 0x16, 0x5a, 0x52, 0xd8, 0x84, 0xa2, 0xbc, 0xf2,
	0xbc, 0xc7, 0xb4, 0x2a, 0x85, 0x39, 0xf1, 0x0a, 0x9a, 0x80, 0x4c, 0x79, 0x13, 0x08, 0x81, 0x15,
	0x7d, 0xa1, 0xab, 0x4f, 0x3f, 0xc9, 0xde, 0xbe, 0xac, 0xea, 0x23, 0xbb, 0x87, 0xad, 0x94, 0xff,
	0x8d, 0x69, 0x4d, 0x57, 0x0d, 0x27, 0x9d, 0xb3, 0xf5, 0xcf, 0xc3, 0x37, 0x97, 0xd6, 0x74, 0xf7,
	0xa1, 0x78, 0xbc, 0x96, 0xfd, 0x3e, 0x02, 0x00, 0x00, 0xff, 0xff, 0xff, 0x16, 0x8d, 0x95, 0x79,
	0x00, 0x00, 0x00,
}
