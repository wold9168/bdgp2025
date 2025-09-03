# Makefile for bdgp2025 project

# Variables
BINARY_CLI=cli
BINARY_SERVER=server
CMD_CLI_DIR=cmd/cli
CMD_SERVER_DIR=cmd/server

# Default target
all: build

# Build all binaries
build: $(BINARY_CLI) $(BINARY_SERVER)

# Build CLI binary
$(BINARY_CLI):
	go build -o $(BINARY_CLI) $(CMD_CLI_DIR)/main.go

# Build Server binary
$(BINARY_SERVER):
	go build -o $(BINARY_SERVER) $(CMD_SERVER_DIR)/main.go

# Install dependencies
deps:
	go mod download

# Clean build artifacts
clean:
	rm -f $(BINARY_CLI) $(BINARY_SERVER)

# Run CLI
run-cli: $(BINARY_CLI)
	./$(BINARY_CLI)

# Run Server
run-server: $(BINARY_SERVER)
	./$(BINARY_SERVER)

# Test the utils package
.PHONY: test
test:
	cd test && go test -v

# Help
help:
	@echo "Available targets:"
	@echo "  all          - Build both binaries (default)"
	@echo "  build        - Build all binaries"
	@echo "  $(BINARY_CLI)    - Build CLI binary"
	@echo "  $(BINARY_SERVER) - Build Server binary"
	@echo "  deps         - Install dependencies"
	@echo "  clean        - Remove build artifacts"
	@echo "  run-cli      - Run CLI binary"
	@echo "  run-server   - Run Server binary"
	@echo "  test         - Run tests"
	@echo "  help         - Show this help message"
