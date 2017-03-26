package stream

import (
	"bufio"
	"fmt"
)

const SlizeSize = 1024

type Streamer interface {
	Stream() ([]uint32, error)
}

func NewScanStreamer(scan *bufio.Scanner) Streamer {
	return &scannerStreamer{scan}
}

type scannerStreamer struct {
	scan *bufio.Scanner
}

func (scan *scannerStreamer) Stream() ([]uint32, error) {
	ret := make([]uint32, 0, SlizeSize)
	for i := 0; scan.scan.Scan() && i < SlizeSize; i++ {
		ret = append(ret, ToUint32(scan.scan.Bytes()))
	}

	if len(ret) == 0 {
		return nil, fmt.Errorf("end of scanner reached")
	}
	return ret, nil
}

func ToUint32(buf []byte) (n uint32) {
	for _, v := range buf {
		n = n*10 + uint32(v-'0')
	}
	return
}
