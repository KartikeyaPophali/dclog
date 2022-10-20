package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
)

// SetupTLSConfig configures a TLS client or server. Possible configurations are:
// 	* Client *tls.Config is set up to verify server's certificate by setting the RootCAs
//	* Client *tls.Config is setup to verify server's certificate and allow server to verify client's certificate
// 		by setting its RootCAs and Certificates
//	* Server *tls.Config is set up to verify client's certificate and allow client to verify server's certificate
//		by setting its ClientCAs, Certificate and ClientAuth mode set to tls.RequireAndVerifyCert
func SetupTLSConfig(cfg TLSConfig) (*tls.Config, error) {
	var err error
	tlsConfig := &tls.Config{}
	if cfg.CertFile != "" && cfg.KeyFile != "" {
		tlsConfig.Certificates = make([]tls.Certificate, 1)
		tlsConfig.Certificates[0], err = tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, err
		}
	}
	if cfg.CAFile != "" {
		b, err := ioutil.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, err
		}
		ca := x509.NewCertPool()
		ok := ca.AppendCertsFromPEM([]byte(b))
		if !ok {
			return nil, fmt.Errorf("failed to parse root certificate: %q", cfg.CAFile)
		}
		if cfg.Server {
			tlsConfig.ClientCAs = ca
			tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		} else {
			tlsConfig.RootCAs = ca
		}
		tlsConfig.ServerName = cfg.ServerAddress
	}
	return tlsConfig, nil
}

type TLSConfig struct {
	CertFile string
	KeyFile string
	CAFile string
	ServerAddress string
	Server bool
}