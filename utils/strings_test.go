package utils_test

import (
	"testing"

	"github.com/msales/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func TestSplitMap(t *testing.T) {
	tests := []struct {
		in     []string
		sep    string
		expect map[string]string
	}{
		{
			in:     []string{"foo=bar", "bar=baz", "test=test=test"},
			sep:    "=",
			expect: map[string]string{"foo": "bar", "bar": "baz", "test": "test=test"},
		},
		{
			in:     []string{},
			sep:    "=",
			expect: nil,
		},
		{
			in:     nil,
			sep:    "=",
			expect: nil,
		},
		{
			in:     []string{"foo=bar", "bar=baz"},
			sep:    "",
			expect: nil,
		},
		{
			in:     []string{"foo", "bar"},
			sep:    "=",
			expect: map[string]string{"foo": "", "bar": ""},
		},
	}

	for _, test := range tests {
		out := utils.SplitMap(test.in, test.sep)
		assert.Equal(t, test.expect, out)
	}
}
