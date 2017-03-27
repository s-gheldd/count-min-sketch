package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"text/tabwriter"

	"time"

	"flag"

	"github.com/s-gheldd/count-min-sketches/count"
	"github.com/s-gheldd/count-min-sketches/countmin"
	"github.com/s-gheldd/count-min-sketches/hash"
	"github.com/s-gheldd/count-min-sketches/stream"
)

var (
	filePaths   = []string{"data/numbers_1M200.txt", "data/numbers_10M1000.txt", "data/numbers_10M100000.txt"}
	sketchWidth uint32
)

func init() {
	var w uint
	flag.UintVar(&w, "w", 13, "the width used by the count-min-sketches")

	flag.Parse()
	sketchWidth = uint32(w)
}

func main() {
	murmurStats := make([]statisic, 0, 3)
	knuthStats := make([]statisic, 0, 3)

	for _, filePath := range filePaths {
		mark1 := time.Now()
		absolutes := count.Count(filePath).First(100)
		mark2 := time.Now()
		log.Println("Absolute count of ", filePath, " took: ", mark2.Sub(mark1))

		sketchKnuth, mark1, mark2 := sketch(filePath, hash.Knuth(4))
		log.Println("count-min-sketch of ", filePath, " using Knuth hashes took: ", mark2.Sub(mark1))

		sketchMurmur, mark1, mark2 := sketch(filePath, hash.Murmur(4))
		log.Println("count-min-sketch of ", filePath, " using Murmur3 hashes took: ", mark2.Sub(mark1))

		knuthStats = append(knuthStats, statistics(absolutes, sketchKnuth))
		murmurStats = append(murmurStats, statistics(absolutes, sketchMurmur))

		fmt.Println()
	}
	printStats(knuthStats, "Knuth")
	printStats(murmurStats, "Murmur3")
}

// knuth better than murmur, more independent hash family?
func printStats(stats []statisic, hash string) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', tabwriter.AlignRight|tabwriter.Debug)

	fmt.Fprintf(w, "%s(w=%d):\n", hash, sketchWidth)
	fmt.Fprintf(w, "data set\tmax. abs.\tavg. abs.\tmax. rel.\tavg. rel.\t# exact\n")

	for i := 0; i < len(stats); i++ {
		stat := stats[i]
		fmt.Fprintf(w, "%s\t%d\t%d\t%.2f\t%.2f\t%d\n", filePaths[i], stat.maxAbs, stat.avgAbs, stat.maxRel, stat.avgRel, 100-stat.misses)
	}
	fmt.Fprintln(w)
	w.Flush()
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

func sketch(filePath string, provider hash.Provider) (*countmin.Sketch, time.Time, time.Time) {

	in, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
	}
	defer in.Close()

	scan := bufio.NewScanner(in)
	var size, intRange uint32

	scan.Scan()
	fmt.Sscan(scan.Text(), &size, &intRange)

	sketch := countmin.NewSketch(sketchWidth, provider)

	before := time.Now()
	for scan.Scan() {
		num := stream.ToUint32(scan.Bytes())
		sketch.Digest(num)
	}
	after := time.Now()
	return sketch, before, after
}
