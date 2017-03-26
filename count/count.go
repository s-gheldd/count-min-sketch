package count

import (
	"bufio"
	"fmt"
	"log"
	"os"

	"sort"

	"github.com/s-gheldd/count-min-sketches/stream"
)

func Count(file string) *Counter {

	in, err := os.Open(file)
	if err != nil {
		log.Fatal(err)
	}
	defer in.Close()

	scan := bufio.NewScanner(in)
	l, r := 0, 0
	scan.Scan()
	fmt.Sscanf(scan.Text(), "%d%d", &l, &r)

	c := &Counter{
		Key:   make([]uint32, r),
		Value: make([]uint32, r),
	}

	for index := 0; index < r; index++ {
		c.Key[index] = uint32(index)
		c.Value[index] = 0
	}

	for scan.Scan() {
		x := stream.ToUint32(scan.Bytes())

		c.Value[x]++
	}

	sort.Sort(c)
	return c
}

type Counter struct {
	Key, Value []uint32
}

func (c *Counter) Len() int {
	return len(c.Key)
}

func (c *Counter) Less(i, j int) bool {
	return c.Value[i] > c.Value[j]
}

func (c *Counter) Swap(i, j int) {
	c.Key[i], c.Key[j] = c.Key[j], c.Key[i]
	c.Value[i], c.Value[j] = c.Value[j], c.Value[i]
}

func (c *Counter) First(n int) *Counter {
	return &Counter{
		Key:   c.Key[:n],
		Value: c.Value[:n],
	}
}

func (c *Counter) String() string {
	concat := "["
	i := 0
	for ; i < len(c.Key)-1; i++ {
		concat += fmt.Sprintf("%d:%d ", c.Key[i], c.Value[i])
	}
	concat += fmt.Sprintf("%d:%d", c.Key[i], c.Value[i]) + "]"

	return concat
}
