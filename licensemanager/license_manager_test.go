package licensemanager

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"github.com/alikonhz/pglogrepl2json/licensemanager/base62"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"strings"
	"testing"
)

func TestSignAndVerify(t *testing.T) {
	pgtestKey := make([]byte, 32) // 32 bytes for AES-256
	_, err := rand.Read(pgtestKey)
	require.NoError(t, err)

	license := License{
		Version:  BinaryVersion,
		Product:  "test product",
		Customer: "test customer",
		Start:    0,
		End:      1000000,
	}

	publicKey, privateKey := GenerateEd25519Keys()
	signer := NewSigner(pgtestKey, privateKey)

	data, err := signer.Sign(license)
	require.NoError(t, err)

	verifier := NewVerifier(pgtestKey, publicKey)

	got, err := verifier.ReadAndVerifyLicense(data)
	require.NoError(t, err)

	assert.Equal(t, license, got)
}

func TestPG2SQSSignAndVerifyJSON(t *testing.T) {
	aesKey, err := hex.DecodeString(AESKey)
	require.NoError(t, err)

	productPrivateKeyName := fmt.Sprintf("./cmd/%s.key", strings.ToLower(string(ProductPG2SQS)))
	_, err = os.Stat(productPrivateKeyName)
	require.NoError(t, err)

	privateKey, err := os.ReadFile(productPrivateKeyName)
	require.NoError(t, err)

	pg2sqsSigner := NewSigner(aesKey, privateKey)

	license := License{
		Version:  JSONVersion,
		Product:  ProductPG2SQS,
		Customer: "Beta user",
		Start:    8888,
		End:      1000000,
	}

	signature, err := pg2sqsSigner.Sign(license)
	require.NoError(t, err)

	base62License, err := base62.EncodeToString(signature)
	require.NoError(t, err)

	publicKey, err := hex.DecodeString(AllProductsPublicKeys[ProductPG2SQS])
	require.NoError(t, err)

	pg2sqsVerifier := NewVerifier(aesKey, publicKey)

	licenseData, err := base62.DecodeString(base62License)
	require.NoError(t, err)

	gotLicense, err := pg2sqsVerifier.ReadAndVerifyLicense(licenseData)
	require.NoError(t, err)

	assert.Equal(t, license, gotLicense)
}
