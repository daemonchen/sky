package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/user"
	"path/filepath"
	"runtime"

	"github.com/skydb/sky/cmd"
	"github.com/skydb/sky/server"
)

var branch, commit string

var config = NewConfig()
var configPath string

func init() {
	log.SetFlags(0)
	flag.UintVar(&config.Port, "port", config.Port, "the port to listen on")
	flag.UintVar(&config.Port, "p", config.Port, "the port to listen on")
	flag.StringVar(&config.DataDir, "data-dir", config.DataDir, "the data directory (defaults to ~/.sky)")
	flag.BoolVar(&config.NoSync, "no-sync", config.NoSync, "use mdb.NOSYNC option, or not")
	flag.UintVar(&config.MaxDBs, "max-dbs", config.MaxDBs, "max number of named btrees in the database (mdb.MaxDBs)")
	flag.UintVar(&config.MaxReaders, "max-readers", config.MaxReaders, "max number of concurrenly executing queries (mdb.MaxReaders)")
	flag.StringVar(&configPath, "config", "", "the path to the config file")
}

func main() {
	// Parse the command line arguments and load the config file (if specified).
	flag.Parse()
	if configPath != "" {
		file, err := os.Open(configPath)
		if err != nil {
			fmt.Printf("Unable to open config: %v\n", err)
			return
		}
		defer file.Close()
		if err = config.Decode(file); err != nil {
			fmt.Printf("Unable to parse config: %v\n", err)
			os.Exit(1)
		}
	}

	// Default the data directory to ~/.sky
	if config.DataDir == "" {
		u, err := user.Current()
		if err != nil {
			log.Fatal(err)
		}
		config.DataDir = filepath.Join(u.HomeDir, ".sky")
	}

	// Hardcore parallelism right here.
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Initialize
	s := server.NewServer(config.Port, config.DataDir)
	s.Version = cmd.Version()
	s.NoSync = config.NoSync
	s.MaxDBs = config.MaxDBs
	s.MaxReaders = config.MaxReaders

	// Print configuration.
	log.Printf("Sky %s (%s %s)", cmd.Version(), branch, commit)
	log.Printf("Listening on http://localhost%s", s.Addr)
	log.Println("")
	log.Println("[config]")
	log.Printf("port        = %v", config.Port)
	log.Printf("data-dir    = %v", config.DataDir)
	log.Printf("no-sync     = %v", s.NoSync)
	log.Printf("max-dbs     = %v", s.MaxDBs)
	log.Printf("max-readers = %v", s.MaxReaders)
	log.Println("")

	// Start the server.
	log.SetFlags(log.LstdFlags)
	log.Fatal(s.ListenAndServe())
}
