package licensemanager

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestMarshalUnmarshalBinaryLicense(t *testing.T) {
	license := License{
		Version:  BinaryVersion,
		Product:  "PG2ANYTHING",
		Customer: "verylongcustomeremailname@somecoolcompany.com",
		Start:    LicenseTime(time.Since(epochStart).Seconds()),
		End:      LicenseTime(time.Now().Add(24 * 7 * 365 * time.Hour).Sub(epochStart).Seconds()),
	}

	data, err := license.Marshal()
	require.NoError(t, err)

	expectedLen := uint16Size                    // version
	expectedLen += uint32Size                    // totalSize
	expectedLen += uint32Size                    // size of the product
	expectedLen += uint32(len(license.Product))  //nolint
	expectedLen += uint32Size                    // size of the customer len
	expectedLen += uint32(len(license.Customer)) //nolint
	expectedLen += uint64Size                    // start
	expectedLen += uint64Size                    // end

	assert.Len(t, data, int(expectedLen))

	got, err := UnmarshalLicense(data)
	require.NoError(t, err)

	assert.Equal(t, license, got)
}

func TestMarshalUnmarshalJSONLicense(t *testing.T) {
	license := License{
		Version:  JSONVersion,
		Product:  "PG2ANYTHING product",
		Customer: "customer109231082@ihaveemailfromsomecoolcompany.com",
		Start:    LicenseTime(time.Since(epochStart).Seconds()),
		End:      LicenseTime(time.Now().Add(24 * 7 * 365 * time.Hour).Sub(epochStart).Seconds()),
	}

	data, err := license.Marshal()
	require.NoError(t, err)

	got, err := UnmarshalLicense(data)
	require.NoError(t, err)

	assert.Equal(t, license, got)
}
