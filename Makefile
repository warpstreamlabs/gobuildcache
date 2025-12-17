.PHONY: all build test clean test-integration

# Binary name
BINARY_NAME=gobuildcache

# Build directory
BUILD_DIR=./builds

all: build test

# Build the cache program
build:
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/$(BINARY_NAME) .

# Run tests with the cache program
test-manual: build
	@echo "Running tests with cache program..."
	GOCACHEPROG="$(shell pwd)/$(BUILD_DIR)/$(BINARY_NAME)" DEBUG=true go test -v ./tests

# Clean build artifacts
clean:
	@echo "Cleaning..."
	rm -f $(BUILD_DIR)/$(BINARY_NAME)
	rm -rf $(BUILD_DIR)/cache

# Run the cache server directly
run: build
	DEBUG=true $(BUILD_DIR)/$(BINARY_NAME)

# Clear the cache
clear: build
	DEBUG=true $(BUILD_DIR)/$(BINARY_NAME) clear

test:
	@echo "Running tests..."
	go test -count 1 -v -race .

