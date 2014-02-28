package main

import (
	"github.com/BurntSushi/toml"
	"io"
)

const (
	DefaultPort       = 8585
	DefaultDataPath   = "/var/lib/sky"
	DefaultNoSync     = false
	DefaultMaxDBs     = 4096
	DefaultMaxReaders = 126 // lmdb's default
)

// Config represents the configuration settings used to start skyd.
type Config struct {
	Port       uint   `toml:"port"`
	DataPath   string `toml:"data-path"`
	NoSync     bool   `toml:"nosync"`
	MaxDBs     uint   `toml:"max-dbs"`
	MaxReaders uint   `toml:"max-readers"`
}

// NewConfig creates a new Config object with the default settings.
func NewConfig() *Config {
	return &Config{
		Port:       DefaultPort,
		DataPath:   DefaultDataPath,
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
