package docker

import (
	"encoding/binary"
	"fmt"
	"io"
)

type multiplexedReader struct {
	reader     io.Reader
	readBuffer []byte
}

// Read reads exactly one stdout frame from the hijacked Docker connection and unwraps it into p. If excess data is
// read, it is stored in c.readBuffer.
//
// We can't read directly from the connection since in non-TTY mode the data is multiplexed to contain both stdout and
// stderr. TTY mode, in contrast, is meant for human consumption and doesn't distinguish between stdout and stderr,
// everything comes in a single data stream with formatting attached, which is also why we can't use it for ATP.
//
// See: https://docs.docker.com/engine/api/v1.41/#tag/Container/operation/ContainerAttach
func (m *multiplexedReader) Read(p []byte) (n int, err error) {
	if len(m.readBuffer) > 0 {
		// We don't need to read more, just return what we have.
		if len(p) < len(m.readBuffer) {
			n = len(p)
		} else {
			n = len(m.readBuffer)
		}
		copy(p, m.readBuffer[:n])
		m.readBuffer = m.readBuffer[n:]
		return n, nil
	}
	header := make([]byte, 8)
	n, err = m.reader.Read(header)
	if err != nil {
		return 0, err
	}
	if n < 8 {
		return 0, fmt.Errorf(
			"incorrect stream frame header from Docker daemon, read %d bytes instead of 8",
			n,
		)
	}
	streamType := header[0]

	frameSize := binary.BigEndian.Uint32(header[4:])
	data := make([]byte, frameSize)
	n, err = io.ReadFull(m.reader, data)
	if err != nil {
		return n, err
	}
	switch streamType {
	case 0:
		return 0, fmt.Errorf(
			"unexpected stdin stream frame from Docker daemon (%s)",
			data[:n],
		)
	case 1:
		// noop
	case 2:
		return 0, fmt.Errorf(
			"unexpected stderr stream frame from Docker daemon (%s)",
			data[:n],
		)
	default:
		return 0, fmt.Errorf(
			"unknown stream frame type frame from Docker daemon: %d",
			streamType,
		)
	}
	m.readBuffer = append(m.readBuffer, data...)
	readLen := 0
	if len(p) > len(m.readBuffer) {
		readLen = len(m.readBuffer)
	} else {
		readLen = len(p)
	}
	copy(p[:readLen], m.readBuffer[:readLen])
	m.readBuffer = m.readBuffer[readLen:]
	return readLen, nil
}
