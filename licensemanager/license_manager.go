package licensemanager

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"time"
	"unsafe"

	"crypto/ed25519"
)

var (
	epochStart = time.Date(2025, time.April, 2, 0, 0, 0, 0, time.UTC)

	uint64Size = uint32(unsafe.Sizeof(uint64(0)))
	uint16Size = uint32(unsafe.Sizeof(uint16(0)))
	uint32Size = uint32(unsafe.Sizeof(uint32(0)))
)

const (
	BinaryVersion uint16 = 1
	JSONVersion          = 2
	nonceLength   int    = 12
)

var (
	currentLicense License
)

func SetLicense(license License) {
	currentLicense = license
}

func CurrentLicense() License {
	return currentLicense
}

var (
	ErrTimeLessThanEpoch = errors.New("time less than epoch start")
)

func CalculateLicenseTime(realTime time.Time) (LicenseTime, error) {
	if realTime.Before(epochStart) {
		return 0, ErrTimeLessThanEpoch
	}

	return LicenseTime(realTime.Sub(epochStart).Seconds()), nil
}

type Signer struct {
	aesKey     []byte
	privateKey ed25519.PrivateKey
}

func NewSigner(aesKey []byte, privateKey ed25519.PrivateKey) *Signer {
	return &Signer{
		aesKey:     aesKey,
		privateKey: privateKey,
	}
}

func GenerateEd25519Keys() (ed25519.PublicKey, ed25519.PrivateKey) {
	publicKey, privateKey, _ := ed25519.GenerateKey(rand.Reader)

	return publicKey, privateKey
}

func (lm *Signer) Sign(license License) ([]byte, error) {
	encrypted, err := lm.encryptLicense(license)
	if err != nil {
		return nil, err
	}

	signature := ed25519.Sign(lm.privateKey, encrypted)

	return append(signature, encrypted...), nil
}

func (lm *Signer) encryptLicense(license License) ([]byte, error) {
	licenseData, err := license.Marshal()
	if err != nil {
		return nil, err
	}

	block, err := aes.NewCipher(lm.aesKey)
	if err != nil {
		return nil, fmt.Errorf("%w: newcipher failed: %w", ErrEncryptionFailed, err)
	}

	// Generate a random nonce (12 bytes for AES-GCM)
	nonce := make([]byte, nonceLength)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("%w: nonce failed: %w", ErrEncryptionFailed, err)
	}

	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("%w: newGCM failed: %w", ErrEncryptionFailed, err)
	}

	encrypted := aesGCM.Seal(nil, nonce, licenseData, nil)

	return append(nonce, encrypted...), nil // nozero
}

type Verifier struct {
	aesKey    []byte
	publicKey ed25519.PublicKey
}

func NewVerifier(aesKey []byte, publicKey ed25519.PublicKey) *Verifier {
	return &Verifier{
		aesKey:    aesKey,
		publicKey: publicKey,
	}
}

var (
	ErrSignatureVerificationFailed = errors.New("signature verification failed")
	ErrDecryptionFailed            = errors.New("decryption failed")
	ErrEncryptionFailed            = errors.New("encryption failed")
	noLicense                      = License{Version: 0, Product: "no_license", Customer: "no_customer", Start: 0, End: 0}
)

func (v *Verifier) ReadAndVerifyLicense(licenseData []byte) (License, error) {
	signature, encryptedData := licenseData[0:64], licenseData[64:]

	if !ed25519.Verify(v.publicKey, encryptedData, signature) {
		return noLicense, ErrSignatureVerificationFailed
	}

	licenseData, err := v.decrypt(encryptedData)
	if err != nil {
		return noLicense, err
	}

	return UnmarshalLicense(licenseData)
}

func (v *Verifier) decrypt(encrypted []byte) ([]byte, error) {
	// Ensure data length is valid
	if len(encrypted) < nonceLength {
		return nil, fmt.Errorf("%w: encrypted data is too short", ErrDecryptionFailed)
	}

	// Extract nonce and encryptedLicenseData
	nonce, encryptedLicenseData := encrypted[:12], encrypted[12:]

	block, err := aes.NewCipher(v.aesKey)
	if err != nil {
		return nil, fmt.Errorf("%w: newcipher failed: %w", ErrDecryptionFailed, err)
	}

	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("%w: newGCM failed: %w", ErrDecryptionFailed, err)
	}

	decrypted, err := aesGCM.Open(nil, nonce, encryptedLicenseData, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: decrypt failed: %w", ErrDecryptionFailed, err)
	}

	return decrypted, nil
}
