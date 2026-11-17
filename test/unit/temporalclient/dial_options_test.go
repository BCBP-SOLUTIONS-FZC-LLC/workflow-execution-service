package temporalclient_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/interceptor"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporalclient"
)

func writeClientKeyPair(t *testing.T) (certPath, keyPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "execution-service"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)

	dir := t.TempDir()
	certPath = filepath.Join(dir, "tls.crt")
	keyPath = filepath.Join(dir, "tls.key")
	require.NoError(t, os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600))
	require.NoError(t, os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600))
	return certPath, keyPath
}

func TestDialOptions_WithoutACertificateDialsInPlainText(t *testing.T) {
	opts, err := temporalclient.DialOptions("temporal:7233", "workflow", "", "", nil)

	require.NoError(t, err)
	assert.Equal(t, "temporal:7233", opts.HostPort)
	assert.Equal(t, "workflow", opts.Namespace)
	assert.Nil(t, opts.ConnectionOptions.TLS)
}

func TestDialOptions_WithAKeyPairPresentsItAsTheClientCertificate(t *testing.T) {
	certPath, keyPath := writeClientKeyPair(t)

	opts, err := temporalclient.DialOptions("temporal:7233", "workflow", certPath, keyPath, nil)

	require.NoError(t, err)
	require.NotNil(t, opts.ConnectionOptions.TLS)
	require.Len(t, opts.ConnectionOptions.TLS.Certificates, 1)
	leaf, err := x509.ParseCertificate(opts.ConnectionOptions.TLS.Certificates[0].Certificate[0])
	require.NoError(t, err)
	assert.Equal(t, "execution-service", leaf.Subject.CommonName)
	assert.GreaterOrEqual(t, opts.ConnectionOptions.TLS.MinVersion, uint16(tls.VersionTLS12))
}

func TestDialOptions_KeepsTheInterceptorsItIsGiven(t *testing.T) {
	given := []interceptor.ClientInterceptor{&interceptor.ClientInterceptorBase{}}

	opts, err := temporalclient.DialOptions("temporal:7233", "workflow", "", "", given)

	require.NoError(t, err)
	assert.Equal(t, given, opts.Interceptors)
}

func TestDialOptions_RefusesAnUnreadableOrHalfConfiguredKeyPair(t *testing.T) {
	certPath, keyPath := writeClientKeyPair(t)
	cases := map[string][2]string{
		"missing files": {filepath.Join(t.TempDir(), "none.crt"), filepath.Join(t.TempDir(), "none.key")},
		"cert only":     {certPath, ""},
		"key only":      {"", keyPath},
		"mismatched":    {certPath, writeOtherKey(t)},
	}
	for name, paths := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := temporalclient.DialOptions("temporal:7233", "workflow", paths[0], paths[1], nil)

			require.Error(t, err)
			assert.Contains(t, err.Error(), "temporal client certificate")
		})
	}
}

func writeOtherKey(t *testing.T) string {
	t.Helper()
	_, keyPath := writeClientKeyPair(t)
	return keyPath
}
