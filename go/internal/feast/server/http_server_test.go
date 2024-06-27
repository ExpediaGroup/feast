package server

import (
	prototypes "github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestUnmarshalJSON(t *testing.T) {
	u := repeatedValue{}
	assert.Nil(t, u.UnmarshalJSON([]byte("[1, 2, 3]")))
	assert.Equal(t, []int64{1, 2, 3}, u.int64Val)

	u = repeatedValue{}
	assert.Nil(t, u.UnmarshalJSON([]byte("[1.2, 2.3, 3.4]")))
	assert.Equal(t, []float64{1.2, 2.3, 3.4}, u.doubleVal)

	u = repeatedValue{}
	assert.Nil(t, u.UnmarshalJSON([]byte("[\"foo\", \"bar\"]")))
	assert.Equal(t, []string{"foo", "bar"}, u.stringVal)

	u = repeatedValue{}
	assert.Nil(t, u.UnmarshalJSON([]byte("[true, false, true]")))
	assert.Equal(t, []bool{true, false, true}, u.boolVal)

	u = repeatedValue{}
	assert.Nil(t, u.UnmarshalJSON([]byte("[[1, 2, 3], [4, 5, 6]]")))
	assert.Equal(t, [][]int64{{1, 2, 3}, {4, 5, 6}}, u.int64ListVal)

	u = repeatedValue{}
	assert.Nil(t, u.UnmarshalJSON([]byte("[[1.2, 2.3, 3.4], [10.2, 20.3, 30.4]]")))
	assert.Equal(t, [][]float64{{1.2, 2.3, 3.4}, {10.2, 20.3, 30.4}}, u.doubleListVal)

	u = repeatedValue{}
	assert.Nil(t, u.UnmarshalJSON([]byte("[[\"foo\", \"bar\"], [\"foo2\", \"bar2\"]]")))
	assert.Equal(t, [][]string{{"foo", "bar"}, {"foo2", "bar2"}}, u.stringListVal)

	u = repeatedValue{}
	assert.Nil(t, u.UnmarshalJSON([]byte("[[true, false, true], [false, true, false]]")))
	assert.Equal(t, [][]bool{{true, false, true}, {false, true, false}}, u.boolListVal)
}

func testTypecastToCorrectTypeWithInt32Val(t *testing.T) {
	val := repeatedValue{}
	val.int64Val = append(val.int64Val, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
	typecastToEntitySchemaType(&val, prototypes.ValueType_INT32)
	assert.Equal(t, nil, val.int64Val)
	assert.NotEqual(t, nil, val.int32Val)
}
