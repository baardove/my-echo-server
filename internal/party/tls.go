package party

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// LoadOrCreateSelfSignedCert loads or generates a persistent self-signed TLS cert in tlsDir
// with SANs for localhost, 127.0.0.1, and advertiseHost so mobile Safari/Chrome can record DJ voice intros over HTTPS.
func LoadOrCreateSelfSignedCert(tlsDir, advertiseHost string) (tls.Certificate, error) {
	if err := os.MkdirAll(tlsDir, 0o700); err != nil {
		return tls.Certificate{}, err
	}
	keyTag := strings.NewReplacer(":", "_", "/", "_").Replace(advertiseHost)
	if keyTag == "" {
		keyTag = "local"
	}
	certPath := filepath.Join(tlsDir, "party-"+keyTag+".crt")
	keyPath := filepath.Join(tlsDir, "party-"+keyTag+".key")

	if cert, err := tls.LoadX509KeyPair(certPath, keyPath); err == nil {
		return cert, nil
	}

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}

	serial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	tmpl := x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   "My Echo Partymode",
			Organization: []string{"My Echo Server"},
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(3 * 365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}

	if advertiseHost != "" {
		if ip := net.ParseIP(advertiseHost); ip != nil {
			tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
		} else {
			tmpl.DNSNames = append(tmpl.DNSNames, advertiseHost)
		}
	}

	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	if err != nil {
		return tls.Certificate{}, err
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyBytes, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		return tls.Certificate{}, err
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes})

	_ = os.WriteFile(certPath, certPEM, 0o644)
	_ = os.WriteFile(keyPath, keyPEM, 0o600)

	return tls.X509KeyPair(certPEM, keyPEM)
}
