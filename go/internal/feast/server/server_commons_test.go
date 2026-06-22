//go:build !integration

package server

import (
	"sort"
	"testing"

	"github.com/feast-dev/feast/go/internal/feast/model"
	"github.com/stretchr/testify/assert"
)

func sortedStrings(s []string) []string {
	out := make([]string, len(s))
	copy(out, s)
	sort.Strings(out)
	return out
}

func TestExtractFVNamesFromRequest_FeatureRefs(t *testing.T) {
	names := extractFVNamesFromRequest([]string{"hotel_fv:price", "hotel_fv:rating", "user_fv:age"}, nil)
	assert.Equal(t, []string{"hotel_fv", "user_fv"}, sortedStrings(names))
}

func TestExtractFVNamesFromRequest_NoColon(t *testing.T) {
	// refs without ":" are ignored (not a valid feature ref)
	names := extractFVNamesFromRequest([]string{"hotel_fv_price"}, nil)
	assert.Empty(t, names)
}

func TestExtractFVNamesFromRequest_FeatureService(t *testing.T) {
	fs := &model.FeatureService{
		Projections: []*model.FeatureViewProjection{
			{Name: "hotel_fv", NameAlias: ""},
			{Name: "user_fv", NameAlias: ""},
		},
	}
	names := extractFVNamesFromRequest(nil, fs)
	assert.Equal(t, []string{"hotel_fv", "user_fv"}, sortedStrings(names))
}

func TestExtractFVNamesFromRequest_Deduplication(t *testing.T) {
	names := extractFVNamesFromRequest([]string{"hotel_fv:price", "hotel_fv:rating"}, nil)
	assert.Equal(t, []string{"hotel_fv"}, names)
}

func TestExtractFVNamesFromRequest_Empty(t *testing.T) {
	names := extractFVNamesFromRequest(nil, nil)
	assert.Empty(t, names)
}
