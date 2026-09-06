package licensemanager

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"time"
)

type Product string
type LicenseTime uint64

func (lt LicenseTime) AsDate() time.Time {
	year, month, day := epochStart.Add(time.Duration(lt) * time.Second).Date()

	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

var (
	ProductPG2SQS   = Product("pg2sqs")
	ProductPG2REDIS = Product("pg2redis")

	AllProductsPublicKeys = map[Product]string{
		ProductPG2SQS:   "1f61afae08769fc2be99ac36386da948ebfcb006cd26172df0151f837899bda9",
		ProductPG2REDIS: "f8b3ae33c3e2e8b83e36774642ebc936df2434f5de0c3f2758471335f1a7adc9",
	}
)

var (
	ErrInvalidVersion = errors.New("license version is invalid")
)

const AESKey = "46b95ecd40a4ffc6b5214813050ba30c34077040ba3f77be49138683bc7a2991"

type License struct {
	Version  uint16
	Product  Product
	Customer string
	Start    LicenseTime
	End      LicenseTime
}

func (l License) Marshal() ([]byte, error) {
	if l.Version == BinaryVersion {
		return l.marshalBinary()
	}

	if l.Version == JSONVersion {
		return l.marshalJSON()
	}

	return nil, ErrInvalidVersion
}

func (l License) marshalJSON() ([]byte, error) {
	j, err := json.Marshal(l)
	if err != nil {
		return nil, err
	}

	jsonWithVersion := make([]byte, len(j)+int(uint16Size))
	binary.BigEndian.PutUint16(jsonWithVersion, l.Version)
	copy(jsonWithVersion[uint16Size:], j)

	return jsonWithVersion, nil
}

func (l License) marshalBinary() ([]byte, error) {
	prodLen := uint32(len(l.Product))  //nolint
	custLen := uint32(len(l.Customer)) //nolint

	totalSize := uint16Size // version
	totalSize += uint32Size // totalSize
	totalSize += uint32Size // size of the product
	totalSize += prodLen
	totalSize += uint32Size // size of the customer len
	totalSize += custLen
	totalSize += uint64Size // start
	totalSize += uint64Size // end

	fullLic := make([]byte, totalSize)
	lic := fullLic

	binary.BigEndian.PutUint16(lic, l.Version)
	lic = lic[uint16Size:] // after Version

	binary.BigEndian.PutUint32(lic, totalSize)
	lic = lic[uint32Size:] // after totalSize

	// write size of the Product field
	binary.BigEndian.PutUint32(lic, prodLen)
	lic = lic[uint32Size:] // after prodLen which is uint32

	copy(lic, []byte(l.Product))
	lic = lic[prodLen:]

	// write size of the Customer field
	binary.BigEndian.PutUint32(lic, custLen)
	lic = lic[uint32Size:] // after custLen which is uint32

	copy(lic, []byte(l.Customer))
	lic = lic[custLen:]

	binary.BigEndian.PutUint64(lic, uint64(l.Start))
	lic = lic[uint64Size:]

	binary.BigEndian.PutUint64(lic, uint64(l.End))

	return fullLic, nil
}

func UnmarshalLicense(data []byte) (License, error) {
	version := binary.BigEndian.Uint16(data[:uint16Size])
	data = data[uint16Size:]

	if version == BinaryVersion {
		return unmarshalBinaryLicense(data)
	}

	if version == JSONVersion {
		return unmarshalJSONLicense(data)
	}

	return noLicense, ErrInvalidVersion
}

func unmarshalJSONLicense(data []byte) (License, error) {
	var license License
	err := json.Unmarshal(data, &license)

	if err != nil {
		return noLicense, err
	}

	license.Version = JSONVersion

	return license, nil
}

func unmarshalBinaryLicense(data []byte) (License, error) {
	_ = binary.BigEndian.Uint32(data[:uint32Size]) // totalSize

	data = data[uint32Size:]

	prodLen := binary.BigEndian.Uint32(data[:uint32Size])
	data = data[uint32Size:]

	product := make([]byte, prodLen)
	copy(product, data[:int(prodLen)])
	data = data[int(prodLen):]

	custLen := binary.BigEndian.Uint32(data[:uint32Size])
	data = data[uint32Size:]

	customer := make([]byte, custLen)
	copy(customer, data[:int(custLen)])
	data = data[int(custLen):]

	start := binary.BigEndian.Uint64(data[:uint64Size])
	data = data[uint64Size:]
	end := binary.BigEndian.Uint64(data[:uint64Size])

	return License{
		Version:  BinaryVersion,
		Product:  Product(product),
		Customer: string(customer),
		Start:    LicenseTime(start),
		End:      LicenseTime(end),
	}, nil
}
