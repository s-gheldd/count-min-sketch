package countmin

import "github.com/s-gheldd/count-min-sketches/hash"

type Sketch struct {
	Funcs  hash.Provider
	Width  uint32
	Counts [][]uint32
}

func NewSketch(width uint32, prov hash.Provider) *Sketch {
	counts := make([][]uint32, len(prov))
	for i := range prov {
		counts[i] = make([]uint32, 2<<width)
	}
	return &Sketch{
		Funcs:  prov,
		Width:  width,
		Counts: counts,
	}
}

func (s *Sketch) Digest(a uint32) {
	for i, f := range s.Funcs {
		hash := f(a)
		s.Counts[i][hash>>(31-s.Width)]++
	}
}

func (s *Sketch) DigestN(a uint32, i int) {
	hash := s.Funcs[i](a)
	s.Counts[i][hash>>(31-s.Width)]++
}

func (s *Sketch) GetCountMin(a uint32) uint32 {
	counts := make([]uint32, len(s.Counts))
	for i, f := range s.Funcs {
		counts[i] = s.Counts[i][f(a)>>(31-s.Width)]
	}
	min := counts[0]
	for _, c := range counts[0:] {
		if c < min {
			min = c
		}
	}
	return min
}
