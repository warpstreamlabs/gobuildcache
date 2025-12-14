package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	if len(os.Args) < 2 {
		// No subcommand, run the server
		runServer()
		return
	}

	subcommand := os.Args[1]

	switch subcommand {
	case "clear":
		clearCmd := flag.NewFlagSet("clear", flag.ExitOnError)
		clearCmd.Usage = func() {
			fmt.Fprintf(os.Stderr, "Usage: %s clear\n", os.Args[0])
			fmt.Fprintf(os.Stderr, "Clear all entries from the cache.\n")
		}
		clearCmd.Parse(os.Args[2:])
		runClear()

	default:
		fmt.Fprintf(os.Stderr, "Unknown subcommand: %s\n\n", subcommand)
		fmt.Fprintf(os.Stderr, "Usage: %s [command]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Commands:\n")
		fmt.Fprintf(os.Stderr, "  (no command)  Run the cache server\n")
		fmt.Fprintf(os.Stderr, "  clear         Clear all entries from the cache\n")
		os.Exit(1)
	}
}

func runServer() {
	cacheDir := getCacheDir()
	debug := getDebugMode()

	// Create disk backend
	backend, err := NewDiskBackend(cacheDir, debug)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating cache backend: %v\n", err)
		os.Exit(1)
	}
	defer backend.Close()

	// Create and run cache program
	prog := NewCacheProg(backend, debug)
	if err := prog.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error running cache program: %v\n", err)
		os.Exit(1)
	}
}

func runClear() {
	cacheDir := getCacheDir()
	debug := getDebugMode()

	// Create disk backend
	backend, err := NewDiskBackend(cacheDir, debug)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating cache backend: %v\n", err)
		os.Exit(1)
	}
	defer backend.Close()

	// Clear the cache
	if err := backend.Clear(); err != nil {
		fmt.Fprintf(os.Stderr, "Error clearing cache: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stdout, "Cache cleared successfully\n")
}

func getCacheDir() string {
	cacheDir := os.Getenv("CACHE_DIR")
	if cacheDir == "" {
		cacheDir = filepath.Join(os.TempDir(), "gobuildcache")
	}
	return cacheDir
}

func getDebugMode() bool {
	debugEnv := strings.ToLower(os.Getenv("DEBUG"))
	return debugEnv == "true"
}
