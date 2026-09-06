package licensemanager

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestParseDateFromString(t *testing.T) {
	tests := []struct {
		date         string
		expectedDate time.Time
	}{
		{
			date:         "2025-05-27",
			expectedDate: time.Date(2025, 5, 27, 0, 0, 0, 0, time.UTC),
		},
		{
			date:         "today",
			expectedDate: time.Now().UTC(),
		},
		{
			date:         "+month",
			expectedDate: time.Now().UTC().AddDate(0, 1, -1),
		},
		{
			date:         "+2months",
			expectedDate: time.Now().UTC().AddDate(0, 2, -1),
		},
		{
			date:         "+year",
			expectedDate: time.Now().UTC().AddDate(1, 0, -1),
		},
		{
			date:         "+2years",
			expectedDate: time.Now().UTC().AddDate(2, 0, -1),
		},
	}

	for _, test := range tests {
		t.Run(test.date, func(t *testing.T) {
			got, err := ParseDate(test.date)
			require.NoError(t, err)
			assert.Equal(t, test.expectedDate.Year(), got.Year())
			assert.Equal(t, test.expectedDate.Month(), got.Month())
			assert.Equal(t, test.expectedDate.Day(), got.Day())
		})
	}
}
