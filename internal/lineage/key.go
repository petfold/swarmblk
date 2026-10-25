package lineage

import (
	"crypto/ecdsa"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethersphere/bee/v2/pkg/crypto"
)

// Lineage keys live in a directory, $SWARMBLK_HOME/keys (SPEC §10), one per
// file named <owner>.key, holding the private key in hex. Nothing here logs
// or prints a key; errors name files and owners only.

// Create starts a lineage: a new key, saved in dir, and a random lineage ID.
func Create(dir string) (Lineage, *ecdsa.PrivateKey, error) {
	key, owner, err := NewKey(dir)
	if err != nil {
		return Lineage{}, nil, err
	}
	var id [16]byte
	rand.Read(id[:])
	return New(owner, id), key, nil
}

// NewKey creates a lineage key and saves it in dir, which it creates with
// mode 0700 if needed, in a file of mode 0600. It returns the key and its
// owner address.
func NewKey(dir string) (*ecdsa.PrivateKey, common.Address, error) {
	key, err := crypto.GenerateSecp256k1Key()
	if err != nil {
		return nil, common.Address{}, err
	}
	owner, err := crypto.NewDefaultSigner(key).EthereumAddress()
	if err != nil {
		return nil, common.Address{}, err
	}
	raw, err := crypto.EncodeSecp256k1PrivateKey(key)
	if err != nil {
		return nil, common.Address{}, err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, common.Address{}, err
	}
	path := keyPath(dir, owner)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, common.Address{}, err
	}
	_, err = f.WriteString(hex.EncodeToString(raw) + "\n")
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(path)
		return nil, common.Address{}, fmt.Errorf("saving the key of 0x%x: %w", owner[:], err)
	}
	return key, owner, nil
}

// LoadKey reads owner's lineage key from dir. Like ssh, it refuses a key
// file that other users can read or write.
func LoadKey(dir string, owner common.Address) (*ecdsa.PrivateKey, error) {
	path := keyPath(dir, owner)
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("no key for lineage owner 0x%x: %w", owner[:], err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if perm := fi.Mode().Perm(); perm&0o077 != 0 {
		return nil, fmt.Errorf("%s has mode %04o; a lineage key must be private to its user (chmod 600)", path, perm)
	}
	b, err := io.ReadAll(io.LimitReader(f, 1024))
	if err != nil {
		return nil, err
	}
	raw, err := hex.DecodeString(strings.TrimSpace(string(b)))
	if err != nil {
		return nil, fmt.Errorf("%s does not hold a key in hex", path) // the error would quote the file
	}
	key, err := crypto.DecodeSecp256k1PrivateKey(raw)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	got, err := crypto.NewDefaultSigner(key).EthereumAddress()
	if err != nil {
		return nil, err
	}
	if got != owner {
		return nil, fmt.Errorf("%s holds the key of 0x%x", path, got[:])
	}
	return key, nil
}

func keyPath(dir string, owner common.Address) string {
	return filepath.Join(dir, fmt.Sprintf("%x.key", owner[:]))
}
