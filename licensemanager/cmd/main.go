package main

import (
	"crypto/ed25519"
	"encoding/hex"
	"flag"
	"fmt"
	"os"

	"github.com/alikonhz/pglogrepl2json/licensemanager"
	"github.com/alikonhz/pglogrepl2json/licensemanager/base62"

	"strings"
	"time"
)

func main() {
	//pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	//
	//fmt.Println("Public :", hex.EncodeToString(pub))
	//fmt.Println("Private :", hex.EncodeToString(priv))
	//
	//os.WriteFile("pg2redis.key", priv, 0666)
	//return

	signFlagSet := flag.NewFlagSet("sign", flag.ExitOnError)

	verifyFlagSet := flag.NewFlagSet("verify", flag.ExitOnError)

	if len(os.Args) < 2 {
		signFlagSet.Usage()
		verifyFlagSet.Usage()
		os.Exit(1)
	}

	switch strings.ToLower(os.Args[1]) {
	case "sign":
		runSign(os.Args[2:], signFlagSet)
	case "verify":
		runVerify(os.Args[2:], verifyFlagSet)
	}

}

func runVerify(args []string, verifyFlagSet *flag.FlagSet) {
	var (
		product string
		input   string
	)

	verifyFlagSet.StringVar(&product, "product", "", "Product name")
	verifyFlagSet.StringVar(&input, "input", "", "input file to verify")

	if err := verifyFlagSet.Parse(args); err != nil {
		fmt.Printf("failed to parse flags: %v\n", err)
		os.Exit(1)
	}

	if product == "" || input == "" {
		verifyFlagSet.Usage()
		os.Exit(1)
	}

	lic, err := os.ReadFile(input)
	if err != nil {
		fmt.Printf("failed to read input file: %v\n", err)
		os.Exit(5)
	}

	license, err := licensemanager.ReadAndVerifyLicenseFromBase62(licensemanager.Product(strings.ToLower(product)), time.Now(), string(lic))
	if err != nil {
		fmt.Printf("failed to verify license: %v\n", err)
		os.Exit(2)
	}

	fmt.Println("license:")
	fmt.Printf("Product: %s\n", license.Product)
	fmt.Printf("Customer: %s\n", license.Customer)
	fmt.Printf("Start: %d (%s)\n", license.Start, license.Start.AsDate().Format(time.DateOnly))
	fmt.Printf("End: %d (%s)\n", license.End, license.End.AsDate().Format(time.DateOnly))
}

func runSign(args []string, signFlagSet *flag.FlagSet) {
	var (
		product  string
		customer string
		start    string
		end      string

		output  string
		licType string
	)

	signFlagSet.StringVar(&product, "product", "", "Product name")
	signFlagSet.StringVar(&customer, "customer", "", "Customer email")
	signFlagSet.StringVar(&start, "start", "", "License start")
	signFlagSet.StringVar(&end, "end", "", "License end")
	signFlagSet.StringVar(&output, "output", "", "Output file")
	signFlagSet.StringVar(&licType, "type", "json", "License type (json or binary)")

	err := signFlagSet.Parse(args)
	if err != nil {
		fmt.Printf("error parsing flags: %v\n", err)
		os.Exit(1)
	}

	if product == "" || customer == "" || start == "" || end == "" {
		signFlagSet.Usage()
		os.Exit(1)
	}

	if _, err := os.Stat(output); err == nil || os.IsExist(err) {
		fmt.Printf("output license file %q already exists\n", output) //nolint
		os.Exit(2)                                                    //nolint
	}

	publicKeyStr, ok := licensemanager.AllProductsPublicKeys[licensemanager.Product(strings.ToLower(product))]
	if !ok {
		fmt.Printf("%q is not a valid product\n", product) //nolint
		os.Exit(3)                                         //nolint
	}

	productPrivateKeyName := fmt.Sprintf("%s.key", strings.ToLower(product))
	if _, err := os.Stat(productPrivateKeyName); os.IsNotExist(err) {
		fmt.Printf("private key file %q does not exist\n", productPrivateKeyName)
		os.Exit(3)
	}

	privateKey, err := os.ReadFile(productPrivateKeyName)
	if err != nil {
		fmt.Printf("error reading private key file %q: %v\n", productPrivateKeyName, err)
		os.Exit(4)
	}

	_, err = hex.DecodeString(publicKeyStr)
	if err != nil {
		fmt.Printf("error decoding public key %q: %v\n", publicKeyStr, err)
		os.Exit(3)
	}

	startDate, err := licensemanager.ParseDate(start)
	if err != nil {
		fmt.Println(err)
		os.Exit(4)
	}

	endDate, err := licensemanager.ParseDateFromTime(end, startDate)
	if err != nil {
		fmt.Println(err)
		os.Exit(4)
	}

	fmt.Println("Generating license for:")
	fmt.Printf("Product: %s\n", product)
	fmt.Printf("Customer: %s\n", customer)
	fmt.Printf("From %q until %q\n", startDate.Format(time.DateOnly), endDate.Format(time.DateOnly))

	licenceStart, err := licensemanager.CalculateLicenseTime(startDate)
	if err != nil {
		fmt.Println(err)
		os.Exit(4)
	}

	licenceEnd, err := licensemanager.CalculateLicenseTime(endDate)
	if err != nil {
		fmt.Println(err)
		os.Exit(4)
	}

	var licVersion uint16
	if strings.EqualFold(licType, "json") {
		licVersion = licensemanager.JSONVersion
	} else {
		licVersion = licensemanager.BinaryVersion
	}

	license := licensemanager.License{
		Version:  licVersion,
		Product:  licensemanager.Product(product),
		Customer: customer,
		Start:    licenceStart,
		End:      licenceEnd,
	}

	aesKey, err := hex.DecodeString(licensemanager.AESKey)
	if err != nil {
		fmt.Printf("failed to decode AES key %q: %v\n", licensemanager.AESKey, err)
		os.Exit(4)
	}

	signer := licensemanager.NewSigner(aesKey, ed25519.PrivateKey(privateKey))

	signature, err := signer.Sign(license)
	if err != nil {
		fmt.Printf("failed to sign license: %v\n", err)
		os.Exit(4)
	}

	base62License, err := base62.EncodeToString(signature)
	if err != nil {
		fmt.Printf("failed to convert license to base62: %v\n", err)
		os.Exit(4)
	}

	if output != "" {
		err = os.WriteFile(output, []byte(base62License), 0644)
		if err != nil {
			fmt.Printf("error writing license to file %q: %v\n", output, err)
			os.Exit(4)
		}

		fmt.Printf("license file written to %q\n", output)
	}

	fmt.Println(base62License)
}
