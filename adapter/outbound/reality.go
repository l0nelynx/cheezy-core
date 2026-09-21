package outbound

import (
	"crypto/ecdh"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"

	tlsC "github.com/metacubex/mihomo/component/tls"
)

type RealityOptions struct {
	PublicKey     string `proxy:"public-key"`
	ShortID       string `proxy:"short-id,omitempty"`
	Mldsa65Verify string `proxy:"mldsa65-verify,omitempty"`
}

func (o RealityOptions) Parse() (*tlsC.RealityConfig, error) {
	if o.PublicKey != "" {
		config := new(tlsC.RealityConfig)

		const x25519ScalarSize = 32
		publicKey, err := base64.RawURLEncoding.DecodeString(o.PublicKey)
		if err != nil || len(publicKey) != x25519ScalarSize {
			return nil, errors.New("invalid REALITY public key")
		}
		config.PublicKey, err = ecdh.X25519().NewPublicKey(publicKey)
		if err != nil {
			return nil, fmt.Errorf("fail to create REALITY public key: %w", err)
		}

		n := hex.DecodedLen(len(o.ShortID))
		if n > tlsC.RealityMaxShortIDLen {
			return nil, errors.New("invalid REALITY short id")
		}
		n, err = hex.Decode(config.ShortID[:], []byte(o.ShortID))
		if err != nil || n > tlsC.RealityMaxShortIDLen {
			return nil, errors.New("invalid REALITY short ID")
		}

		if o.Mldsa65Verify != "" {
			config.Mldsa65Verify, err = base64.RawURLEncoding.DecodeString(o.Mldsa65Verify)
			if err != nil || len(config.Mldsa65Verify) != 1952 {
				return nil, errors.New("invalid REALITY ML-DSA-65 verification key")
			}
		}
		return config, nil
	}
	if o.Mldsa65Verify != "" {
		return nil, errors.New("REALITY ML-DSA-65 verification requires a public key")
	}
	return nil, nil
}
