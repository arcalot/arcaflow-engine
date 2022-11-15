package docker //nolint:testpackage
import (
	"bytes"
	"encoding/binary"
	"testing"
)

func encodeFrame(streamType byte, data []byte) []byte {
	encoded := make([]byte, len(data)+8)
	copy(encoded[8:], data)
	encoded[0] = streamType
	binary.BigEndian.PutUint32(encoded[4:8], uint32(len(data)))
	return encoded
}

func TestReaderOnEmpty(t *testing.T) {
	data := encodeFrame(1, []byte("Hello world!"))
	r := multiplexedReader{
		reader: bytes.NewReader(data),
	}
	readData := make([]byte, 255)
	n, err := r.Read(readData)
	if err != nil {
		t.Fatalf("Error while reading from multiplexed reader: %v", err)
	}
	if n != 12 {
		t.Fatalf("Incorrect number of bytes read: %d", n)
	}
	if string(readData[:n]) != "Hello world!" {
		t.Fatalf("Incorrect data read from multiplexed reader: %s", string(readData[:n]))
	}
}

func TestReaderOnBuffered(t *testing.T) {
	data := encodeFrame(1, []byte(" world!"))
	r := multiplexedReader{
		reader:     bytes.NewReader(data),
		readBuffer: []byte("Hello"),
	}
	readData := make([]byte, 255)
	n, err := r.Read(readData)
	if err != nil {
		t.Fatalf("Error while reading from multiplexed reader: %v", err)
	}
	if n != 5 {
		t.Fatalf("Incorrect number of bytes read: %d", n)
	}
	if string(readData[:n]) != "Hello" { // nolint:goconst
		t.Fatalf("Incorrect data read from multiplexed reader: %s", string(readData[:n]))
	}

	n, err = r.Read(readData)
	if err != nil {
		t.Fatalf("Error while reading from multiplexed reader: %v", err)
	}
	if n != 7 {
		t.Fatalf("Incorrect number of bytes read: %d", n)
	}
	if string(readData[:n]) != " world!" { // nolint:goconst
		t.Fatalf("Incorrect data read from multiplexed reader: %s", string(readData[:n]))
	}
}

func TestReaderOverread(t *testing.T) {
	data := encodeFrame(1, []byte("Hello world!"))
	r := multiplexedReader{
		reader: bytes.NewReader(data),
	}
	readData := make([]byte, 5)
	n, err := r.Read(readData)
	if err != nil {
		t.Fatalf("Error while reading from multiplexed reader: %v", err)
	}
	if n != 5 {
		t.Fatalf("Incorrect number of bytes read: %d", n)
	}
	if string(readData[:n]) != "Hello" {
		t.Fatalf("Incorrect data read from multiplexed reader: %s", string(readData[:n]))
	}
	if string(r.readBuffer[:7]) != " world!" {
		t.Fatalf("Incorrect read buffer contents: %s", string(r.readBuffer[:7]))
	}
}

func TestReaderOverreadBuffered(t *testing.T) {
	data := encodeFrame(1, []byte(" world!"))
	r := multiplexedReader{
		reader:     bytes.NewReader(data),
		readBuffer: []byte("Hello"),
	}
	readData := make([]byte, 512)
	n, err := r.Read(readData)
	if err != nil {
		t.Fatalf("Error while reading from multiplexed reader: %v", err)
	}
	if n != 5 {
		t.Fatalf("Incorrect number of bytes read: %d", n)
	}
	if string(readData[:n]) != "Hello" {
		t.Fatalf("Incorrect data read from multiplexed reader: %s", string(readData[:n]))
	}
	n, err = r.Read(readData)
	if err != nil {
		t.Fatalf("Error while reading from multiplexed reader: %v", err)
	}
	if n != 7 {
		t.Fatalf("Incorrect number of bytes read: %d", n)
	}
	if string(readData[:n]) != " world!" {
		t.Fatalf("Incorrect data read from multiplexed reader: %s", string(readData[:n]))
	}
}

func TestReaderOnlyBuffered(t *testing.T) {
	data := encodeFrame(1, []byte(" world!"))
	r := multiplexedReader{
		reader:     bytes.NewReader(data),
		readBuffer: []byte("Hello"),
	}
	readData := make([]byte, 5)
	n, err := r.Read(readData)
	if err != nil {
		t.Fatalf("Error while reading from multiplexed reader: %v", err)
	}
	if n != 5 {
		t.Fatalf("Incorrect number of bytes read: %d", n)
	}
	if string(readData[:n]) != "Hello" {
		t.Fatalf("Incorrect data read from multiplexed reader: %s", string(readData[:n]))
	}
	if len(r.readBuffer) != 0 {
		t.Fatalf("Incorrect read buffer length despite enough data previously: %d", len(r.readBuffer))
	}
}

func TestReaderIncorrectStreamType(t *testing.T) {
	data := encodeFrame(2, []byte("Hello world!"))
	r := multiplexedReader{
		reader: bytes.NewReader(data),
	}
	readData := make([]byte, 5)
	n, err := r.Read(readData)
	if err == nil {
		t.Fatalf("Expected error, but got %d bytes", n)
	}
}
