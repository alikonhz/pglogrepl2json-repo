package config

import (
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"
)

var (
	ErrHostsPortsCountMismatch = errors.New("number of ports should match number of hosts or should be exactly one")
)

type ConnectionOptsMany struct {
	Host     string     `json:"host"`
	Port     string     `json:"port"`
	Database string     `json:"database"`
	User     string     `json:"user"`
	Password string     `json:"password"`
	TLS      TLSOptions `json:"tls"`

	opts []ConnectionOpts
}

func JoinOpts(opts []ConnectionOpts) (*ConnectionOptsMany, error) {
	if len(opts) == 0 {
		return nil, nil
	}

	many := &ConnectionOptsMany{
		Host:     opts[0].Host,
		Port:     opts[0].Port,
		Database: opts[0].Database,
		User:     opts[0].User,
		Password: opts[0].Password,
		TLS:      opts[0].TLS,
	}
	if len(opts) == 1 {
		return many, nil
	}

	hosts := make([]string, len(opts))
	ports := make([]string, len(opts))
	for i := 0; i < len(opts); i++ {
		if opts[i].Database != many.Database ||
			opts[i].User != many.User ||
			opts[i].Password != many.Password {
			return nil, errors.New("database, user and password must be the same")
		}

		hosts[i] = opts[i].Host
		ports[i] = opts[i].Port
	}

	many.Host = strings.Join(hosts, ",")
	many.Port = strings.Join(ports, ",")
	optsCopy := make([]ConnectionOpts, len(opts))
	copy(optsCopy, opts)
	many.opts = optsCopy

	return many, nil
}

func (com *ConnectionOptsMany) AsOpt() ConnectionOpts {
	return ConnectionOpts{
		Host:     com.Host,
		Port:     com.Port,
		Database: com.Database,
		User:     com.User,
		Password: com.Password,
		TLS:      com.TLS,
	}
}

func (com *ConnectionOptsMany) AllOpts() ([]ConnectionOpts, error) {
	if com.opts == nil {
		opts, err := com.parseOpts()
		if err != nil {
			return nil, err
		}

		com.opts = opts
	}

	return com.opts, nil
}

func (com *ConnectionOptsMany) parseOpts() ([]ConnectionOpts, error) {
	hosts := strings.Split(com.Host, ",")
	ports := strings.Split(com.Port, ",")
	if com.Host == "" || len(hosts) == 0 {
		return nil, errors.New("no hosts provided")
	}
	if com.Port == "" || len(ports) == 0 {
		return nil, errors.New("no ports provided")
	}

	// skip empty hosts and ports
	hosts = slices.DeleteFunc(hosts, func(s string) bool {
		return strings.TrimSpace(s) == ""
	})
	ports = slices.DeleteFunc(ports, func(s string) bool {
		return strings.TrimSpace(s) == ""
	})

	hosts = trim(hosts)
	ports = trim(ports)

	if len(hosts) != len(ports) && len(ports) > 1 {
		return nil, ErrHostsPortsCountMismatch
	}

	opts := make([]ConnectionOpts, len(hosts))
	for i := 0; i < len(hosts); i++ {
		host := hosts[i]
		var port string
		if len(ports) > 1 {
			port = ports[i]
		} else {
			port = ports[0]
		}

		opts[i] = ConnectionOpts{
			Host:     host,
			Port:     port,
			Database: com.Database,
			User:     com.User,
			Password: com.Password,
			TLS:      com.TLS,
		}
	}

	return opts, nil
}

func trim(s []string) []string {
	for i := 0; i < len(s); i++ {
		s[i] = strings.TrimSpace(s[i])
	}

	return s
}

type ConnectionOpts struct {
	Host     string     `json:"host"`
	Port     string     `json:"port"`
	Database string     `json:"database"`
	User     string     `json:"user"`
	Password string     `json:"password"`
	TLS      TLSOptions `json:"tls"`
}

type TLSOptions struct {
	Cert     string `json:"cert"`
	Key      string `json:"key"`
	RootCert string `json:"rootCert"`
}

func (to TLSOptions) IsEmpty() bool {
	return to.Cert == "" &&
		to.Key == "" &&
		to.RootCert == ""
}

func (to TLSOptions) Validate() error {
	if to.IsEmpty() {
		return nil
	}

	var err error
	if to.Cert == "" {
		err = errors.New("TLS cert is required")
	}
	if to.Key == "" {
		err = errors.Join(err, errors.New("TLS key is required"))
	}
	if to.RootCert == "" {
		err = errors.Join(err, errors.New("TLS root cert is required"))
	}

	return err
}

func (opt ConnectionOpts) CreateConnStr() string {
	u := &url.URL{
		Scheme: "postgres",
		Host:   fmt.Sprintf("%s:%s", opt.Host, opt.Port),
		Path:   opt.Database,
	}

	if opt.Password != "" {
		u.User = url.UserPassword(opt.User, opt.Password)
	} else {
		u.User = url.User(opt.User)
	}

	if !opt.TLS.IsEmpty() {
		q := u.Query()
		q.Set("sslmode", "require")
		q.Set("sslcert", opt.TLS.Cert)
		q.Set("sslkey", opt.TLS.Key)
		q.Set("sslrootcert", opt.TLS.RootCert)
		u.RawQuery = q.Encode()
	}

	return u.String()
}

//func (opt ConnectionOpts) CreateConfig() (*pgx.ConnConfig, error) {
//	//cfg, _ := pgx.ParseConfig("")
//	//cfg.Host = opt.Host
//	//p, err := strconv.Atoi(opt.Port)
//	//if err != nil {
//	//	return nil, fmt.Errorf("port %s is not a number", opt.Port)
//	//}
//	//
//	//cfg.Port = uint16(p)
//	//cfg.Database = opt.Database
//	//cfg.User = opt.User
//	//cfg.Password = opt.Password
//	//
//	//if !opt.TLS.IsEmpty() {
//	//	cfg.TLSConfig = &tls.Config{
//	//		Certificates: []tls.Certificate{
//	//			{
//	//				Certificate: [][]byte{[]byte(opt.TLS.Cert)},
//	//				PrivateKey:  []byte(opt.TLS.Key),
//	//			},
//	//		},
//	//		RootCAs: x509.NewCertPool(),
//	//	}
//	//}
//	//
//	//return cfg, nil
//
//	// TODO: for the future - we should support other ways of loading TLS options
//	// then we will need to load tls manually
//
//	panic("not implemented yet")
//}
