package model

import (
	"github.com/feast-dev/feast/go/protos/feast/core"
)

type SortKey struct {
	FieldName string
	Order     string
}

func NewSortKeyFromProto(proto *core.SortKey) *SortKey {
	return &SortKey{
		FieldName: proto.GetName(),
		Order:     proto.GetDefaultSortOrder().String(),
	}
}

type SortedFeatureView struct {
	*FeatureView
	SortKeys []*SortKey
}

func NewSortedFeatureViewFromProto(proto *core.SortedFeatureView) *SortedFeatureView {
	// Create a base FeatureView using Spec fields from the proto.
	baseFV := &FeatureView{
		Base: NewBaseFeatureView(proto.GetSpec().GetName(), proto.GetSpec().GetFeatures()),
		Ttl:  proto.GetSpec().GetTtl(),
	}

	// Convert each sort key from the proto.
	sortKeys := make([]*SortKey, len(proto.GetSpec().GetSortKeys()))
	for i, skProto := range proto.GetSpec().GetSortKeys() {
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
