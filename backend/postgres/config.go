package postgres

import (
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"net/url"
	"sync"
)

type DBRole string

const (
	Master  DBRole = "master"
	Replica DBRole = "replica"
)

type SSLMode string

const (
	Disable    SSLMode = "disable"
	Allow      SSLMode = "allow"
	Prefer     SSLMode = "prefer"
	Require    SSLMode = "require"
	VerifyCA   SSLMode = "verify-ca"
	VerifyFull SSLMode = "verify-full"
)

type Config struct {
	Host string
	Port int

	DBName string

	User     string
	Password string

	ApplicationName string

	SSLMode SSLMode
	SSLSNI  bool

	CACertFile string
	// Deprecated: Prefer passing a slice of CACerts
	CACert  *x509.Certificate
	CACerts []*x509.Certificate

	Role DBRole

	certOnce sync.Once
	certErr  error
}

func (c *Config) Driver() string { return "postgres" }
func (c *Config) IsMaster() bool { return c.Role != Replica }

func (c *Config) host() string {
	if c.Host == "" {
		return "localhost"
	}

	return c.Host
}

func (c *Config) port() int {
	if c.Port == 0 {
		return 5432
	}

	return c.Port
}

func (c *Config) userInfo() *url.Userinfo {
	if c.User == "" && c.Password == "" {
		return nil
	}

	if c.Password == "" {
		return url.User(c.User)
	}

	return url.UserPassword(c.User, c.Password)
}

func writeBundle(certs []*x509.Certificate) (string, error) {
	f, err := ioutil.TempFile("", "")

	if err != nil {
		return "", err
	}

	for _, cert := range certs {
		if err := pem.Encode(
			f,
			&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw},
		); err != nil {
			f.Close()
			return "", err
		}
	}

	return f.Name(), f.Close()
}

func (c *Config) caCerts() []*x509.Certificate {
	var certs = c.CACerts

	if c.CACert != nil {
		certs = append(certs, c.CACert)
	}

	return certs
}

func (c *Config) sslValues() (url.Values, error) {
	mode := Disable

	if c.CACertFile == "" && c.CACert != nil {
		c.certOnce.Do(func() { c.CACertFile, c.certErr = writeBundle(c.caCerts()) })

		if c.certErr != nil {
			return nil, c.certErr
		}
	}

	if c.SSLMode != "" {
		mode = c.SSLMode
	} else if c.CACertFile != "" {
		mode = VerifyCA
	}

	vs := url.Values{
		"sslmode": {string(mode)},
		"sslsni":  {"0"},
	}

	if c.SSLSNI {
		vs["sslsni"][0] = "1"
	}

	if c.CACertFile != "" {
		vs["sslrootcert"] = []string{c.CACertFile}
	}

	return vs, nil
}

func (c *Config) DSN() (string, error) {
	var q, err = c.sslValues()

	if err != nil {
		return "", err
	}

	if c.ApplicationName != "" {
		q.Add("application_name", c.ApplicationName)
	}

	u := url.URL{
		Scheme:   "postgres",
		User:     c.userInfo(),
		Host:     fmt.Sprintf("%s:%d", c.host(), c.port()),
		Path:     "/" + c.DBName,
		RawQuery: q.Encode(),
	}

	return u.String(), nil
}
