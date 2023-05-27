package gsvt

import (
	"encoding/binary"
	"math"

	"github.com/drewlanenga/govector"
	"golang.org/x/sync/errgroup"
)

const COSINE = 0
const EUCLIDEAN = 1
const DOT_PRODUCT = 2

type Vector struct {
	Metadata map[string]interface{}
	Vector   govector.Vector
}

type SimilarityOptions struct {
	// Method defines which method is applicable. Expected
	// values is one of these constants:
	// COSINE, EUCLIDEAN, DOT_PRODUCT
	Method int

	// Workers is how many workers to use when calculating
	// the similarity. This value needs to be at least 1
	// or higher. If the values returned needing similarity
	// calculations is less than the number of workers, then
	// we ignore this and use the number of returned values
	// instead.
	Workers int
}

var DefaultSimilarityOptions *SimilarityOptions = &SimilarityOptions{
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
func (v *Vector) SimilarityToVectorSet(vectors []*Vector, options *SimilarityOptions) ([]float64, error) {
	if options == nil {
		options = DefaultSimilarityOptions
	}
	if options.Workers == 0 {
		options.Workers = DefaultSimilarityOptions.Workers
	}

	similarities := make([]float64, len(vectors))
	indexChannel := make(chan int)
	var group errgroup.Group

	workers := options.Workers
	if workers > len(vectors) {
		workers = len(vectors)
	}

	for i := 0; i < workers; i++ {
		group.Go(func() error {
			for index := range indexChannel {
				similarity, err := v.SimilarityToVector(vectors[index], options)
				if err != nil {
					return nil
				}
				similarities[index] = similarity
			}
			return nil
		})
	}

	for index, _ := range vectors {
		indexChannel <- index
	}
	close(indexChannel)

	if err := group.Wait(); err != nil {
		return nil, err
	}

	return similarities, nil
}

// SimilarityToVector - Given a vector, find its similarity
// w/ the specified method. If the options aren't specified,
// DEFAULTOPTIONS will be used.
func (v *Vector) SimilarityToVector(other *Vector, options *SimilarityOptions) (float64, error) {
	if options == nil {
		options = DefaultSimilarityOptions
	}

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

func (v *Vector) cosineSimilarity(vector *Vector) (float64, error) {
	similarity, err := govector.Cosine(v.Vector, vector.Vector)
	return similarity, err
}

func (v *Vector) euclideanDistance(vector *Vector) (float64, error) {
	// sum = v.Vector.Pow(2) + vector.Vector.Pow(2)
	return 0.0, nil
}

func (v *Vector) dotProduct(vector *Vector) (float64, error) {
	return govector.DotProduct(v.Vector, vector.Vector)
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
