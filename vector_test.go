package gsvt

import (
	"testing"

	"github.com/drewlanenga/govector"
)

func TestVectorSimilarity(t *testing.T) {
	vectors := []*Vector{
		{Vector: govector.Vector{1.0, 2.0, 3.0}},
		{Vector: govector.Vector{1.0, 2.0, 3.0}},
		{Vector: govector.Vector{1.0, 2.0, 3.0}},
		{Vector: govector.Vector{1.0, 2.0, 3.0}},
		{Vector: govector.Vector{1.0, 2.0, 3.0}},
		{Vector: govector.Vector{1.0, 2.0, 3.0}},
		{Vector: govector.Vector{1.0, 2.0, 3.0}},
	}
	baseVector := &Vector{Vector: govector.Vector{1.0, 2.0, 3.0}}
	options := &SimilarityOptions{
		Method:  COSINE,
		Workers: 4,
	}

	similarities := baseVector.SimilarityToVectorSet(vectors, options)

	for _, similarity := range similarities {
		if similarity != 1.0 {
			t.Error("Similarity should be 1.0")
		}
	}
}
