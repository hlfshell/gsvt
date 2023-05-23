package gsvt

import (
	"encoding/binary"
	"math"
	"sync"

	"github.com/drewlanenga/govector"
)

const COSINE = 0
const EUCLIDEAN = 1
const DOT_PRODUCT = 2

type Vector struct {
	Metadata map[string]interface{}
	Vector   govector.Vector
}

type SimilarityOptions struct {
	Method  int
	Workers int
}

var DEFAULTOPTIONS *SimilarityOptions = &SimilarityOptions{
	Method:  COSINE,
	Workers: 50,
}

/*
SimilarityToVectorSet - Given a set of vectors, find their
similarity to the base vector and return. The indexes
are preserved.

In the options, Workers is utilized to split this task
out to N workers for a speed-up

If the options aren't specified, DEFAULTOPTIONS will be used.
*/
func (v *Vector) SimilarityToVectorSet(vectors []*Vector, options *SimilarityOptions) []float64 {
	if options == nil {
		options = DEFAULTOPTIONS
	}
	if options.Workers == 0 {
		options.Workers = DEFAULTOPTIONS.Workers
	}

	similarities := make([]float64, len(vectors))
	indexChannel := make(chan int) //, options.Workers)
	var wait sync.WaitGroup

	workers := options.Workers
	if workers > len(vectors) {
		workers = len(vectors)
	}

	for i := 0; i < workers; i++ {
		go func() {
			for index := range indexChannel {
				similarities[index] = v.SimilarityToVector(vectors[index], options)
				wait.Done()
			}
		}()
	}

	for index, _ := range vectors {
		wait.Add(1)
		indexChannel <- index
	}

	close(indexChannel)
	wait.Wait()

	// For now do it the serial method - do worker queue
	// in a bit
	for i, vector := range vectors {
		similarities[i] = v.SimilarityToVector(vector, options)
	}

	return similarities
}

// SimilarityToVector - Given a vector, find its similarity
// w/ the specified method. If the options aren't specified,
// DEFAULTOPTIONS will be used.
func (v *Vector) SimilarityToVector(other *Vector, options *SimilarityOptions) float64 {
	switch options.Method {
	case COSINE:
		return v.cosineSimilarity(other)
	case EUCLIDEAN:
		return v.euclideanDistance(other)
	case DOT_PRODUCT:
		return v.dotProduct(other)
	default:
		return v.cosineSimilarity(other)
	}
}

func (v *Vector) cosineSimilarity(vector *Vector) float64 {
	return 0.0
}

func (v *Vector) euclideanDistance(vector *Vector) float64 {
	return 0.0
}

func (v *Vector) dotProduct(vector *Vector) float64 {
	return 0.0
}

// ToBytes - Convert the vector to a byte array
func (v *Vector) ToBytes() []byte {
	// 8 bytes per float, so we need to allocate N * 8 bytes
	// where N is the length of the vector
	byteArray := make([]byte, len(v.Vector)*8)

	// ...then we convert each float64 to a singular piece of
	// the byte array to the correct location in the array
	for index, value := range v.Vector {
		start := index * 8
		end := start + 8

		binary.LittleEndian.PutUint64(byteArray[start:end], math.Float64bits(value))
	}

	return byteArray
}

// FromBytes sets the vector to a value from a given byte array
func (v *Vector) FromBytes(bytes []byte) {
	// We need to allocate the vector to the correct size
	// based on the length of the byte array
	v.Vector = make(govector.Vector, len(bytes)/8)

	// Then we convert each piece of the byte array to a float64
	// and set the value in the vector
	for index := 0; index < len(bytes); index += 8 {
		start := index
		end := index + 8

		v.Vector[index/8] = math.Float64frombits(
			binary.LittleEndian.Uint64(bytes[start:end]),
		)
	}
}
