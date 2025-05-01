package model

import "time"

type ModelTTL[T any] struct {
	Model T
	ttl   time.Time
}

func (m *ModelTTL[T]) IsExpired() bool {
	if m.ttl.IsZero() {
		return false
	}
	return time.Now().After(m.ttl)
}

func NewModelTTL[T any](model T) *ModelTTL[T] {
	return &ModelTTL[T]{Model: model}
}

func NewModelTTLWithExpiration[T any](model T, ttl time.Duration) *ModelTTL[T] {
	return &ModelTTL[T]{Model: model, ttl: time.Now().Add(ttl)}
}
