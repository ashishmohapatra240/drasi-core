#!/bin/bash
# Building Comfort Dashboard Example
# Builds and runs the example, then opens the dashboard

set -e

echo "╔════════════════════════════════════════════╗"
echo "║   Building Comfort Dashboard Example       ║"
echo "╚════════════════════════════════════════════╝"
echo ""

# Check if ports are available
check_port() {
    local port=$1
    if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1; then
        echo "⚠️  Port $port is already in use"
        read -p "Kill existing process? (y/n) " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            lsof -ti:$port | xargs kill -9 2>/dev/null || true
            echo "✓ Killed process on port $port"
        else
            echo "Please free port $port and try again"
            exit 1
        fi
    fi
}

# Check required ports
check_port 8080
check_port 8081
check_port 9000

echo "Building..."
cargo build

if [ $? -eq 0 ]; then
    echo ""
    echo "Starting Building Comfort Dashboard..."
    echo ""
    echo "┌────────────────────────────────────────────┐"
    echo "│ Dashboard:    http://localhost:8081        │"
    echo "│ SSE Stream:   http://localhost:8080/events │"
    echo "│ HTTP Source:  http://localhost:9000        │"
    echo "└────────────────────────────────────────────┘"
    echo ""

    # Try to open browser (works on macOS)
    if command -v open &> /dev/null; then
        (sleep 2 && open http://localhost:8081) &
    fi

    cargo run
else
    echo "Build failed!"
    exit 1
fi
