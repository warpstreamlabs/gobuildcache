package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gobuildcache/backends"
)

// Global flags
var (
	debug       bool
	backendType string
	cacheDir    string
	s3Bucket    string
	s3Prefix    string
	s3TmpDir    string
)

func main() {
	// Check if we have a subcommand
	if len(os.Args) > 1 && !strings.HasPrefix(os.Args[1], "-") {
		subcommand := os.Args[1]

		switch subcommand {
		case "clear":
			runClearCommand()
			return
		case "help", "-h", "--help":
			printHelp()
			return
		default:
			fmt.Fprintf(os.Stderr, "Unknown subcommand: %s\n\n", subcommand)
			printHelp()
			os.Exit(1)
		}
	}

	// No subcommand or starts with -, run the server
	runServerCommand()
}

func runServerCommand() {
	serverFlags := flag.NewFlagSet("server", flag.ExitOnError)

	serverFlags.BoolVar(&debug, "debug", false, "Enable debug logging to stderr")
	serverFlags.StringVar(&backendType, "backend", "disk", "Backend type (disk, s3)")
	serverFlags.StringVar(&cacheDir, "cache-dir", filepath.Join(os.TempDir(), "gobuildcache"), "Cache directory for disk backend")
	serverFlags.StringVar(&s3Bucket, "s3-bucket", "", "S3 bucket name (required for s3 backend)")
	serverFlags.StringVar(&s3Prefix, "s3-prefix", "", "S3 key prefix (optional)")
	serverFlags.StringVar(&s3TmpDir, "s3-tmp-dir", filepath.Join(os.TempDir(), "gobuildcache-s3"), "Local temp directory for S3 backend")

	serverFlags.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [flags]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Run the Go build cache server.\n\n")
		fmt.Fprintf(os.Stderr, "Flags:\n")
		serverFlags.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  # Run with disk backend:\n")
		fmt.Fprintf(os.Stderr, "  %s -cache-dir=/var/cache/go\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  # Run with S3 backend:\n")
		fmt.Fprintf(os.Stderr, "  %s -backend=s3 -s3-bucket=my-cache-bucket\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  # Run with debug logging:\n")
		fmt.Fprintf(os.Stderr, "  %s -debug\n", os.Args[0])
	}

	serverFlags.Parse(os.Args[1:])
	runServer()
}

func runClearCommand() {
	clearFlags := flag.NewFlagSet("clear", flag.ExitOnError)

	clearFlags.BoolVar(&debug, "debug", false, "Enable debug logging to stderr")
	clearFlags.StringVar(&backendType, "backend", "disk", "Backend type (disk, s3)")
	clearFlags.StringVar(&cacheDir, "cache-dir", filepath.Join(os.TempDir(), "gobuildcache"), "Cache directory for disk backend")
	clearFlags.StringVar(&s3Bucket, "s3-bucket", "", "S3 bucket name (required for s3 backend)")
	clearFlags.StringVar(&s3Prefix, "s3-prefix", "", "S3 key prefix (optional)")
	clearFlags.StringVar(&s3TmpDir, "s3-tmp-dir", filepath.Join(os.TempDir(), "gobuildcache-s3"), "Local temp directory for S3 backend")

	clearFlags.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s clear [flags]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Clear all entries from the cache.\n\n")
		fmt.Fprintf(os.Stderr, "Flags:\n")
		clearFlags.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  # Clear disk cache:\n")
		fmt.Fprintf(os.Stderr, "  %s clear -cache-dir=/var/cache/go\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  # Clear S3 cache:\n")
		fmt.Fprintf(os.Stderr, "  %s clear -backend=s3 -s3-bucket=my-cache-bucket\n", os.Args[0])
	}

	clearFlags.Parse(os.Args[2:])
	runClear()
}

func printHelp() {
	fmt.Fprintf(os.Stderr, "Usage: %s [command] [flags]\n\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "A remote caching server for Go builds.\n\n")
	fmt.Fprintf(os.Stderr, "Commands:\n")
	fmt.Fprintf(os.Stderr, "  (no command)  Run the cache server (default)\n")
	fmt.Fprintf(os.Stderr, "  clear         Clear all entries from the cache\n")
	fmt.Fprintf(os.Stderr, "  help          Show this help message\n\n")
	fmt.Fprintf(os.Stderr, "Run '%s [command] -h' for more information about a command.\n", os.Args[0])
}

func runServer() {
	// Create backend
	backend, err := createBackend()
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
	// Create backend
	backend, err := createBackend()
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

func createBackend() (backends.Backend, error) {
	backendType = strings.ToLower(backendType)

	var backend backends.Backend
	var err error

	switch backendType {
	case "disk":
		backend, err = backends.NewDisk(cacheDir)

	case "s3":
		if s3Bucket == "" {
			return nil, fmt.Errorf("-s3-bucket flag is required for S3 backend")
		}

		backend, err = backends.NewS3(s3Bucket, s3Prefix, s3TmpDir)

	default:
		return nil, fmt.Errorf("unknown backend type: %s (supported: disk, s3)", backendType)
	}

	if err != nil {
		return nil, err
	}

	// Wrap with debug backend if debug mode is enabled
	if debug {
		backend = backends.NewDebug(backend)
	}

	return backend, nil
}
