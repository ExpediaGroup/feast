package model

import (
	"github.com/feast-dev/feast/go/protos/feast/core"
	"github.com/feast-dev/feast/go/protos/feast/types"
)

type Entity struct {
	Name      string
	JoinKey   string
	ValueType types.ValueType_Enum
}

func NewEntityFromProto(proto *core.Entity) *Entity {
	return &Entity{
		Name:      proto.Spec.Name,
		JoinKey:   proto.Spec.JoinKey,
		ValueType: proto.Spec.ValueType,
	}
}
