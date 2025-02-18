package model

import (
	"time"

	durationpb "google.golang.org/protobuf/types/known/durationpb"

	"github.com/feast-dev/feast/go/protos/feast/core"
)

type SortedFeatureView struct {
	*FeatureView
	SortKeys []*SortKey
}

func NewSortedFeatureViewFromProto(proto *core.SortedFeatureView) *SortedFeatureView {
	baseFV := NewFeatureViewFromProto(&core.FeatureView{
		Spec: proto.Spec,
		Meta: proto.Meta,
	})

	sortKeys := make([]*SortKey, len(proto.Spec.SortKeys))
	for i, skProto := range proto.Spec.SortKeys {
		sortKeys[i] = NewSortKeyFromProto(skProto)
	}

	return &SortedFeatureView{
		FeatureView: baseFV,
		SortKeys:    sortKeys,
	}
}

func NewSortedFeatureViewFromStreamProto(proto *core.StreamFeatureView) *SortedFeatureView {
	baseFV := NewFeatureViewFromStreamFeatureViewProto(&core.FeatureView{
		Spec: proto.Spec,
		Meta: proto.Meta,
	})

	sortKeys := make([]*SortKey, len(proto.Spec.SortKeys))
	for i, skProto := range proto.Spec.SortKeys {
		sortKeys[i] = NewSortKeyFromProto(skProto)
	}

	return &SortedFeatureView{
		FeatureView: baseFV,
		SortKeys:    sortKeys,
	}
}

func (sfv *SortedFeatureView) NewSortedFeatureViewFromBase(base *BaseFeatureView) *SortedFeatureView {
	newFV := sfv.FeatureView.NewFeatureViewFromBase(base)
	return &SortedFeatureView{
		FeatureView: newFV,
		SortKeys:    sfv.SortKeys,
	}
}
