package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPatchProductRejectsNegativeValues(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "negative price",
			body: `{"price_cents":-1}`,
			want: `{"error":"price_cents must be non-negative"}`,
		},
		{
			name: "negative stock",
			body: `{"stock_quantity":-1}`,
			want: `{"error":"stock_quantity must be non-negative"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPatch, "/products/1", strings.NewReader(tt.body))
			req.SetPathValue("id", "1")
			res := httptest.NewRecorder()

			(&server{}).patchProduct(res, req)

			require.Equal(t, http.StatusBadRequest, res.Code)
			require.JSONEq(t, tt.want, res.Body.String())
		})
	}
}

func TestSortPositiveIntStringsSortsNumerically(t *testing.T) {
	got, err := sortPositiveIntStrings([]string{"10", "2", "1"})

	require.NoError(t, err)
	require.Equal(t, []string{"1", "2", "10"}, got)
}

func TestSortPositiveIntStringsRejectsInvalidValues(t *testing.T) {
	tests := []string{"0", "-1", "not-a-number"}

	for _, tt := range tests {
		t.Run(tt, func(t *testing.T) {
			_, err := sortPositiveIntStrings([]string{"1", tt})

			require.Error(t, err)
		})
	}
}
