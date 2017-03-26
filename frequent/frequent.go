package frequent

import (
	"bufio"
	"log"
	"os"

	"fmt"

	"github.com/s-gheldd/count-min-sketches/stream"
)

const size = 100

var monitors map[uint32]uint32 = make(map[uint32]uint32, size)

func FillMonitors(file string) {
	in, err := os.Open(file)
	if err != nil {
		log.Fatal(err)
	}
	defer in.Close()

	scan := bufio.NewScanner(in)

	scan.Scan()
	scan.Text()

	for scan.Scan() {
		x := stream.ToUint32(scan.Bytes())
		//count up
		if val, ok := monitors[x]; ok {
			monitors[x] = val + 1

			// do we still have space
		} else if len(monitors) < size {
			monitors[x] = 1

			// we need to decrement
		} else {
			// decrement and find the ones to delete
			for key, val := range monitors {
				if val > 1 {
					monitors[key] = val - 1
				} else {
					//fmt.Println("deleting", key)
					delete(monitors, key)
				}
			}
		}
		//fmt.Println(monitors)

	}
	fmt.Println(len(monitors), "monitors", monitors)
}

func GetAbsolutes(file string) map[uint32]uint32 {
	absolutes := make(map[uint32]uint32, 100)

	for key := range monitors {
		absolutes[key] = 0
	}

	in, err := os.Open(file)
	if err != nil {
		log.Fatal(err)
	}
	defer in.Close()

	scan := bufio.NewScanner(in)

	scan.Scan()
	scan.Text()

	for scan.Scan() {
		x := stream.ToUint32(scan.Bytes())
		//count up
		if val, ok := absolutes[x]; ok {
			absolutes[x] = val + 1
		}
	}

	return absolutes

}
