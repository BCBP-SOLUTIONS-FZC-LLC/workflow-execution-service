package temporalclient

import (
	"crypto/tls"
	"errors"
	"fmt"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
)

func DialOptions(hostPort, namespace, certPath, keyPath string, interceptors []interceptor.ClientInterceptor) (client.Options, error) {
	opts := client.Options{HostPort: hostPort, Namespace: namespace, Interceptors: interceptors}
	if certPath == "" && keyPath == "" {
		return opts, nil
	}
	if certPath == "" || keyPath == "" {
		return client.Options{}, errors.New("temporal client certificate: the certificate and key paths must be set together")
	}
	pair, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return client.Options{}, fmt.Errorf("temporal client certificate: %w", err)
	}
	opts.ConnectionOptions = client.ConnectionOptions{
		TLS: &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12},
	}
	return opts, nil
}
