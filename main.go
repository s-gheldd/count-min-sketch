package main

import (
	"bufio"
	"fmt"
	"log"
	"os"

	"github.com/s-gheldd/count-min-sketches/count"
	"github.com/s-gheldd/count-min-sketches/countmin"
	"github.com/s-gheldd/count-min-sketches/hash"
	"github.com/s-gheldd/count-min-sketches/stream"
)

const filePath = "data/numbers_10M100000.txt"

// func main() {
// 	in, err := os.Open(filePath)
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	defer in.Close()

// 	scan := bufio.NewScanner(in)
// 	var size, intRange uint32

// 	scan.Scan()
// 	fmt.Sscan(scan.Text(), &size, &intRange)

// 	s := countmin.NewSketch(12, hash.Murmur(4))

// 	for scan.Scan() {
// 		num := hash.ToUint32(scan.Bytes())
// 		s.Digest(num)
// 	}

// 	for index := uint32(1); index < intRange; index++ {
// 		fmt.Println(index, "=>", s.GetCountMin(uint32(index)))
// 	}
// }

func main() {

	absolutes := count.Count(filePath).First(100)
	sketch := murmurMin(filePath)

	stat := statistics(absolutes, sketch)
	fmt.Printf("%+v\n", stat)

}

func statistics(topHundret *count.Counter, sketch *countmin.Sketch) statisic {
	var maxAbs, avgAbs, misses uint32
	var maxRel, avgRel float64

	for i := 0; i < topHundret.Len(); i++ {
		count := topHundret.Value[i]
		guess := sketch.GetCountMin(topHundret.Key[i])

		rel := relErr(count, guess)
		abs := absErr(count, guess)

		if abs != 0 {
			misses++
		}

		if maxAbs < abs {
			maxAbs = abs
		}

		if maxRel < rel {
			maxRel = rel
		}

		avgAbs += abs
		avgRel += rel
	}

	avgAbs /= uint32(topHundret.Len())
	avgRel /= float64(topHundret.Len())

	return statisic{maxAbs, avgAbs, misses, maxRel, avgRel}
}

type statisic struct {
	maxAbs, avgAbs, misses uint32
	maxRel, avgRel         float64
}

func relErr(count, guess uint32) float64 {
	if count <= guess {
		return 100.0 * (float64(guess) - float64(count)) / float64(count)
	} else {
		//should not happen
		log.Fatal("guess", guess, "was smaller than count", count)
		return 100.0 * (float64(count) - float64(guess)) / float64(count)
	}
}

func absErr(count, guess uint32) uint32 {
	if count <= guess {
		return guess - count
	} else {
		//should not happen
		log.Fatal("guess", guess, "was smaller than count", count)
		return count - guess
	}
}

func streamer(path string) (stream.Streamer, func() error) {
	in, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
	}

	scan := bufio.NewScanner(in)

	scan.Scan()
	scan.Text()
	streamer := stream.NewScanStreamer(scan)

	return streamer, in.Close
}

func murmurMin(filePath string) *countmin.Sketch {

	in, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
	}
	defer in.Close()

	scan := bufio.NewScanner(in)
	var size, intRange uint32

	scan.Scan()
	fmt.Sscan(scan.Text(), &size, &intRange)

	sketch := countmin.NewSketch(14, hash.Murmur(4))

	for scan.Scan() {
		num := stream.ToUint32(scan.Bytes())
		sketch.Digest(num)
	}
	return sketch
}
