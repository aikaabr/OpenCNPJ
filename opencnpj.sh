#!/bin/bash

# OpenCNPJ - Unified Script
# All-in-one script for OpenCNPJ operations

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_success() { echo -e "${GREEN}✓ $1${NC}"; }
print_error() { echo -e "${RED}✗ $1${NC}"; }
print_warning() { echo -e "${YELLOW}⚠ $1${NC}"; }
print_info() { echo -e "${BLUE}ℹ $1${NC}"; }

# Directories
BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
ETL_DIR="$BASE_DIR/ETL"
ANALYTICS_DIR="$BASE_DIR/Analytics"
WEB_DIR="$BASE_DIR/Page"

# Load environment
if [ -f "$BASE_DIR/.env" ]; then
    set -a
    source "$BASE_DIR/.env"
    set +a
fi

# Defaults
WEB_PORT=${WEB_PORT:-8080}
STORAGE_TYPE=${STORAGE_TYPE:-filesystem}
STORAGE_ENABLED=${STORAGE_ENABLED:-true}

print_header() {
    echo -e "${BLUE}🚀 OpenCNPJ${NC}"
    echo -e "${BLUE}===========${NC}"
    if [ "$STORAGE_TYPE" = "filesystem" ]; then
        echo -e "${GREEN}📁 Mode: Local storage${NC}"
    elif [ "$STORAGE_ENABLED" = "false" ]; then
        echo -e "${YELLOW}🚫 Mode: No storage${NC}"
    else
        echo -e "${BLUE}☁️  Mode: Remote storage ($STORAGE_TYPE)${NC}"
    fi
    echo ""
}

# Check basic dependencies
check_deps() {
    if ! command -v dotnet &> /dev/null; then
        print_error ".NET not found. Run: ./opencnpj.sh setup"
        exit 1
    fi
}

# Setup dependencies
setup_deps() {
    print_info "Setting up OpenCNPJ dependencies..."
    
    # Check OS
    if [[ "$OSTYPE" == "darwin"* ]]; then
        OS="macos"
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        OS="linux"
    else
        print_error "Unsupported OS"
        exit 1
    fi
    
    # Install .NET
    if ! command -v dotnet &> /dev/null; then
        print_info "Installing .NET..."
        if [ "$OS" = "macos" ]; then
            if command -v brew &> /dev/null; then
                brew install --cask dotnet
            else
                print_error "Install Homebrew first, then run: brew install --cask dotnet"
                exit 1
            fi
        else
            print_error "Install .NET manually: https://dotnet.microsoft.com/download"
            exit 1
        fi
    fi
    
    # Install Python & dbt (optional)
    if command -v python3 &> /dev/null; then
        if ! command -v pipx &> /dev/null; then
            print_info "Installing pipx..."
            python3 -m pip install --user pipx
            python3 -m pipx ensurepath
        fi
        
        if ! pipx list | grep -q "dbt-core"; then
            print_info "Installing dbt..."
            pipx install dbt-core
            pipx inject dbt-core dbt-duckdb
        fi
    fi
    
    print_success "Setup complete!"
}

# Build .NET project
build_dotnet() {
    cd "$ETL_DIR"
    if [ ! -f "bin/Release/net9.0/CNPJExporter.dll" ] || [ "CNPJExporter.csproj" -nt "bin/Release/net9.0/CNPJExporter.dll" ]; then
        print_info "Building .NET project..."
        dotnet build -c Release --verbosity quiet
        print_success "Build complete"
    fi
    cd "$BASE_DIR"
}

# ETL operations
run_etl() {
    local cmd=$1
    check_deps
    build_dotnet
    
    cd "$ETL_DIR"
    case $cmd in
        pipeline|p) dotnet run -c Release -- pipeline ;;
        test|t) dotnet run -c Release -- test ;;
        single) dotnet run -c Release -- single "$2" ;;
        zip) dotnet run -c Release -- zip ;;
        *) dotnet run -c Release -- "$cmd" ;;
    esac
    cd "$BASE_DIR"
}

# Analytics with dbt
run_analytics() {
    local cmd=${1:-run}
    
    if ! command -v dbt &> /dev/null; then
        print_warning "dbt not found. Run: ./opencnpj.sh setup"
        return 1
    fi
    
    cd "$ANALYTICS_DIR"
    export DBT_PROFILES_DIR="$ANALYTICS_DIR"
    
    case $cmd in
        run|r) dbt run ;;
        test|t) dbt test ;;
        docs|d) dbt docs generate && dbt docs serve --port 8081 ;;
        debug) dbt debug ;;
        clean) dbt clean ;;
        *) dbt "$cmd" ;;
    esac
    cd "$BASE_DIR"
}

# Web server
run_web() {
    print_info "Starting web server on port $WEB_PORT"
    cd "$WEB_DIR"
    
    if command -v python3 &> /dev/null; then
        python3 -m http.server "$WEB_PORT"
    elif command -v python &> /dev/null; then
        python -m http.server "$WEB_PORT"
    else
        print_error "Python not found for web server"
        exit 1
    fi
}

# Storage configuration
configure_storage() {
    case $1 in
        local|filesystem)
            sed -i.bak 's/STORAGE_TYPE=.*/STORAGE_TYPE=filesystem/' .env
            sed -i.bak 's/STORAGE_ENABLED=.*/STORAGE_ENABLED=true/' .env
            print_success "Configured for local filesystem storage"
            ;;
        disable|none)
            sed -i.bak 's/STORAGE_ENABLED=.*/STORAGE_ENABLED=false/' .env
            print_success "Storage disabled - local processing only"
            ;;
        rclone)
            sed -i.bak 's/STORAGE_TYPE=.*/STORAGE_TYPE=rclone/' .env
            sed -i.bak 's/STORAGE_ENABLED=.*/STORAGE_ENABLED=true/' .env
            print_warning "Configure rclone settings in .env"
            ;;
        s3)
            sed -i.bak 's/STORAGE_TYPE=.*/STORAGE_TYPE=s3/' .env
            sed -i.bak 's/STORAGE_ENABLED=.*/STORAGE_ENABLED=true/' .env
            print_warning "Configure S3 settings in .env"
            ;;
        *)
            print_error "Unknown storage type. Use: local, disable, rclone, s3"
            exit 1
            ;;
    esac
}

# Clean builds
clean_all() {
    print_info "Cleaning builds..."
    
    # .NET
    if [ -d "$ETL_DIR/bin" ] || [ -d "$ETL_DIR/obj" ]; then
        cd "$ETL_DIR" && dotnet clean && cd "$BASE_DIR"
    fi
    
    # Python venv
    [ -d ".venv" ] && rm -rf .venv
    
    # dbt
    if [ -d "$ANALYTICS_DIR" ] && command -v dbt &> /dev/null; then
        cd "$ANALYTICS_DIR" && dbt clean 2>/dev/null || true && cd "$BASE_DIR"
    fi
    
    print_success "Clean complete"
}

# Help
show_help() {
    print_header
    echo "Usage: ./opencnpj.sh [COMMAND] [OPTIONS]"
    echo ""
    echo "MAIN COMMANDS:"
    echo "  pipeline, p      - Run full ETL pipeline"
    echo "  analytics, a     - Run dbt analytics"
    echo "  web, w           - Start web server"
    echo "  all              - Run pipeline + analytics + web"
    echo ""
    echo "ETL COMMANDS:"
    echo "  etl <cmd>        - Run ETL command (pipeline, test, zip, single <cnpj>)"
    echo ""
    echo "ANALYTICS COMMANDS:"
    echo "  dbt <cmd>        - Run dbt command (run, test, docs, debug, clean)"
    echo ""
    echo "UTILITY COMMANDS:"
    echo "  setup            - Install dependencies"
    echo "  storage <type>   - Configure storage (local, disable, rclone, s3)"
    echo "  clean            - Clean all builds"
    echo "  help             - Show this help"
    echo ""
    echo "EXAMPLES:"
    echo "  ./opencnpj.sh p              # Run pipeline"
    echo "  ./opencnpj.sh etl single 123 # Process specific CNPJ"
    echo "  ./opencnpj.sh storage local  # Use local storage only"
    echo "  ./opencnpj.sh storage disable # Disable all storage"
    echo "  ./opencnpj.sh all            # Run everything"
    echo ""
    echo "STORAGE: ${STORAGE_TYPE} (enabled: ${STORAGE_ENABLED})"
}

# Main execution
main() {
    case ${1:-help} in
        # Main commands
        pipeline|p) run_etl pipeline ;;
        analytics|a) run_analytics ;;
        web|w) run_web ;;
        all) 
            run_etl pipeline
            command -v dbt &> /dev/null && run_analytics
            print_info "Starting web server..."
            run_web
            ;;
        
        # Specific commands
        etl) shift; run_etl "$@" ;;
        dbt) shift; run_analytics "$@" ;;
        
        # Utility commands
        setup) setup_deps ;;
        storage) shift; configure_storage "$1" ;;
        clean) clean_all ;;
        
        # Help
        help|--help|-h|"") show_help ;;
        
        # Unknown
        *) 
            print_error "Unknown command: $1"
            echo ""
            show_help
            exit 1
            ;;
    esac
}

# Run main function
main "$@"