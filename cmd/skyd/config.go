package main

import (
	"io"

	"github.com/BurntSushi/toml"
)

const (
	// DefaultPort is the port that Sky listens on by default.
	DefaultPort = 8585

	// DefaultNoSync is the default setting for syncing the data.
	DefaultNoSync = false

	// DefaultMaxDBs is the default LMDB setting for MaxDBs.
	DefaultMaxDBs = 4096

	// DefaultMaxReaders is the default LMDB setting for MaxReaders.
	DefaultMaxReaders = 126 // lmdb's default
)

// Config represents the configuration settings used to start skyd.
type Config struct {
	Port       uint   `toml:"port"`
	DataDir    string `toml:"data-dir"`
	NoSync     bool   `toml:"no-sync"`
	MaxDBs     uint   `toml:"max-dbs"`
	MaxReaders uint   `toml:"max-readers"`
}

// NewConfig creates a new Config object with the default settings.
func NewConfig() *Config {
	return &Config{
		Port:       DefaultPort,
		NoSync:     DefaultNoSync,
		MaxDBs:     DefaultMaxDBs,
		MaxReaders: DefaultMaxReaders,
	}
}

// Decode reads the contents of configuration file and populates the config object.
// Any properties that are not set in the configuration file will default to
// the value of the property before the decode.
func (c *Config) Decode(r io.Reader) error {
	if _, err := toml.DecodeReader(r, &c); err != nil {
		return err
	}
	return nil
}
