package certs

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"time"
)

func GenerateCerts(dir string) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	// CA
	ca := &x509.Certificate{
		SerialNumber: big.NewInt(2026),
		Subject: pkix.Name{
			Organization: []string{"Test CA"},
			CommonName:   "Test Root CA",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(10, 0, 0),
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}

	caPrivKey, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return err
	}

	caBytes, err := x509.CreateCertificate(rand.Reader, ca, ca, &caPrivKey.PublicKey, caPrivKey)
	if err != nil {
		return err
	}

	if err := savePEM(filepath.Join(dir, "ca.crt"), "CERTIFICATE", caBytes, 0644); err != nil {
		return err
	}

	// Server Cert
	serverCert := &x509.Certificate{
		SerialNumber: big.NewInt(2027),
		Subject: pkix.Name{
			Organization: []string{"Test Server"},
			CommonName:   "localhost",
		},
		DNSNames:     []string{"localhost"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().AddDate(10, 0, 0),
		SubjectKeyId: []byte{1, 2, 3, 4, 6},
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}

	serverPrivKey, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return err
	}

	serverBytes, err := x509.CreateCertificate(rand.Reader, serverCert, ca, &serverPrivKey.PublicKey, caPrivKey)
	if err != nil {
		return err
	}

	if err := savePEM(filepath.Join(dir, "server.crt"), "CERTIFICATE", serverBytes, 0644); err != nil {
		return err
	}
	if err := savePEM(filepath.Join(dir, "server.key"), "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(serverPrivKey), 0600); err != nil {
		return err
	}

	// Client Cert
	clientCert := &x509.Certificate{
		SerialNumber: big.NewInt(2028),
		Subject: pkix.Name{
			Organization: []string{"Test Client"},
			CommonName:   "postgres",
		},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().AddDate(10, 0, 0),
		SubjectKeyId: []byte{1, 2, 3, 4, 7},
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}

	clientPrivKey, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return err
	}

	clientBytes, err := x509.CreateCertificate(rand.Reader, clientCert, ca, &clientPrivKey.PublicKey, caPrivKey)
	if err != nil {
		return err
	}

	if err := savePEM(filepath.Join(dir, "client.crt"), "CERTIFICATE", clientBytes, 0644); err != nil {
		return err
	}
	if err := savePEM(filepath.Join(dir, "client.key"), "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(clientPrivKey), 0600); err != nil {
		return err
	}

	return nil
}

func savePEM(filename, pemType string, b []byte, mode os.FileMode) error {
	var buf bytes.Buffer
	if err := pem.Encode(&buf, &pem.Block{Type: pemType, Bytes: b}); err != nil {
		return err
	}
	return os.WriteFile(filename, buf.Bytes(), mode)
}
