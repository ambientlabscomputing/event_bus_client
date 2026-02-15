package event_bus_client

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/asn1"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"time"
)

// MTLSTransport wraps http.RoundTripper to add mTLS headers
type MTLSTransport struct {
	base           http.RoundTripper
	privateKey     *ecdsa.PrivateKey
	certificatePEM []byte
}

// RoundTrip implements http.RoundTripper
func (t *MTLSTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Clone the request to avoid modifying the original
	reqClone := req.Clone(req.Context())

	// Add certificate header (base64 encoded)
	certBase64 := base64.StdEncoding.EncodeToString(t.certificatePEM)
	reqClone.Header.Set("X-Client-Certificate", certBase64)

	// Add timestamp for replay protection
	timestamp := time.Now().UTC().Format(time.RFC3339)
	reqClone.Header.Set("X-Request-Timestamp", timestamp)

	// Read request body for signing
	var bodyBytes []byte
	if req.Body != nil {
		var err error
		bodyBytes, err = io.ReadAll(req.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read request body: %w", err)
		}
		// Restore body for the actual request
		reqClone.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	}

	// Build signature payload: METHOD\nPATH\nTIMESTAMP\nBODY
	signaturePayload := buildSignaturePayload(req.Method, req.URL.Path, timestamp, bodyBytes)

	// Sign the payload
	signature, err := signData(t.privateKey, signaturePayload)
	if err != nil {
		return nil, fmt.Errorf("failed to sign request: %w", err)
	}

	// Add signature header (base64 encoded)
	signatureBase64 := base64.StdEncoding.EncodeToString(signature)
	reqClone.Header.Set("X-Client-Signature", signatureBase64)

	// Make the request
	return t.base.RoundTrip(reqClone)
}

// buildSignaturePayload creates the data that should be signed
// Format: METHOD\nPATH\nTIMESTAMP\nBODY
func buildSignaturePayload(method, path, timestamp string, body []byte) []byte {
	var buf bytes.Buffer
	buf.WriteString(method)
	buf.WriteString("\n")
	buf.WriteString(path)
	buf.WriteString("\n")
	buf.WriteString(timestamp)
	buf.WriteString("\n")
	buf.Write(body)
	return buf.Bytes()
}

// ECDSASignature represents an ECDSA signature in ASN.1 DER format
type ECDSASignature struct {
	R, S *big.Int
}

// signData signs data with a private key and returns ASN.1 DER encoded signature
func signData(privateKey *ecdsa.PrivateKey, data []byte) ([]byte, error) {
	if privateKey == nil {
		return nil, fmt.Errorf("private key is nil")
	}

	// Hash the data
	hash := sha256.Sum256(data)

	// Sign the hash
	r, s, err := ecdsa.Sign(rand.Reader, privateKey, hash[:])
	if err != nil {
		return nil, fmt.Errorf("failed to sign data: %w", err)
	}

	// Encode signature as ASN.1 DER
	sig := ECDSASignature{R: r, S: s}
	signatureDER, err := asn1.Marshal(sig)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal signature: %w", err)
	}

	return signatureDER, nil
}

// initMTLS initializes mTLS authentication by loading certificate and private key
func (ec *Client) initMTLS() error {
	// Load private key
	keyPEM, err := os.ReadFile(ec.opts.KeyPath)
	if err != nil {
		return fmt.Errorf("failed to read private key file: %w", err)
	}

	privateKey, err := parsePrivateKey(keyPEM)
	if err != nil {
		return fmt.Errorf("failed to parse private key: %w", err)
	}

	// Load certificate
	certPEM, err := os.ReadFile(ec.opts.CertPath)
	if err != nil {
		return fmt.Errorf("failed to read certificate file: %w", err)
	}

	// Validate certificate can be parsed
	if err := validateCertificate(certPEM); err != nil {
		return fmt.Errorf("invalid certificate: %w", err)
	}

	ec.privateKey = privateKey
	ec.certificatePEM = certPEM
	ec.mtlsEnabled = true

	return nil
}

// parsePrivateKey parses a PEM-encoded ECDSA private key
func parsePrivateKey(keyPEM []byte) (*ecdsa.PrivateKey, error) {
	block, _ := pem.Decode(keyPEM)
	if block == nil {
		return nil, fmt.Errorf("failed to decode PEM block")
	}

	// Try PKCS8 format first (most common)
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err == nil {
		if ecdsaKey, ok := key.(*ecdsa.PrivateKey); ok {
			return ecdsaKey, nil
		}
		return nil, fmt.Errorf("key is not ECDSA")
	}

	// Try EC private key format
	ecdsaKey, err := x509.ParseECPrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key: %w", err)
	}

	return ecdsaKey, nil
}

// validateCertificate validates that the PEM-encoded certificate is valid
func validateCertificate(certPEM []byte) error {
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return fmt.Errorf("failed to decode PEM block")
	}

	_, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return fmt.Errorf("failed to parse certificate: %w", err)
	}

	return nil
}

// getWebSocketHeaders returns headers for WebSocket connection with mTLS
func (ec *Client) getWebSocketHeaders() http.Header {
	if !ec.mtlsEnabled {
		return nil
	}

	headers := http.Header{}

	// Add certificate header (base64 encoded)
	certBase64 := base64.StdEncoding.EncodeToString(ec.certificatePEM)
	headers.Set("X-Client-Certificate", certBase64)

	return headers
}
