package licensemanager

import (
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/alikonhz/pglogrepl2json/licensemanager/base62"
)

var (
	ErrLicNotFound      = errors.New("license not found")
	ErrLicFileNotExist  = errors.New("license file does not exist")
	ErrLicFileReadError = errors.New("failed to read license file")
)

func ReadAndVerifyLicenseFromBase62(product Product, today time.Time, licBase62 string) (License, error) {
	publicKeyStr, ok := AllProductsPublicKeys[product]
	if !ok {
		return noLicense, fmt.Errorf("%q is not a valid product\n", product)
	}

	aesKey, err := hex.DecodeString(AESKey)
	if err != nil {
		return noLicense, fmt.Errorf("failed to decode AES key")
	}

	publicKey, err := hex.DecodeString(publicKeyStr)
	if err != nil {
		return noLicense, fmt.Errorf("failed to decode public key")
	}

	verifier := NewVerifier(aesKey, publicKey)

	licenseData, err := base62.DecodeString(licBase62)
	if err != nil {
		return noLicense, fmt.Errorf("failed to parse license: %v", err)
	}

	license, err := verifier.ReadAndVerifyLicense(licenseData)
	if err != nil {
		return noLicense, fmt.Errorf("failed to verify license: %v", err)
	}

	licStart := license.Start.AsDate()
	licEnd := license.End.AsDate()

	if !today.Before(licStart) && today.Before(licEnd) {
		return license, nil
	}

	return noLicense, fmt.Errorf("license is not valid at %s", today.Format("2006-01-02"))
}

func VerifyLicenseFromBase62(product Product, today time.Time, licBase62 string) error {
	_, err := ReadAndVerifyLicenseFromBase62(product, today, licBase62)

	return err
}

func VerifyAndSetLicenseFromEnv(product Product, today time.Time) error {
	var (
		licenseFromEnv string
		err            error
	)

	licenseFromEnv = os.Getenv(fmt.Sprintf("PGWALK_LIC_%s", strings.ToUpper(string(product))))
	if licenseFromEnv == "" {
		licenseFromEnv, err = tryReadFromLicenseFile(product)

		if err != nil {
			return err
		}
	}

	if licenseFromEnv == "" {
		return ErrLicNotFound
	}

	lic, err := ReadAndVerifyLicenseFromBase62(product, today, licenseFromEnv)
	if err != nil {
		return err
	}

	SetLicense(lic)

	return nil
}

func tryReadFromLicenseFile(product Product) (string, error) {
	licFile := os.Getenv(fmt.Sprintf("PGWALK_LICFILE_%s", strings.ToUpper(string(product))))
	if licFile == "" {
		return "", nil
	}

	data, err := os.ReadFile(licFile)
	if err != nil {
		if os.IsNotExist(err) {
			return "", ErrLicFileNotExist
		}

		return "", ErrLicFileReadError
	}

	return strings.TrimSpace(string(data)), nil
}
