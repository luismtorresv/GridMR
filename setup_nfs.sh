#!/bin/bash
"""
NFS Setup Script for GridMR MapReduce System
This script helps set up NFS server and client configurations for distributed deployment
"""

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_header() {
    echo -e "${BLUE}===============================================${NC}"
    echo -e "${BLUE}    GridMR NFS Setup for AWS Deployment${NC}"
    echo -e "${BLUE}===============================================${NC}"
    echo
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

setup_nfs_server() {
    echo -e "${BLUE}Setting up NFS Server (Master Node)...${NC}"

    # Install NFS server
    sudo apt-get update
    sudo apt-get install -y nfs-kernel-server

    # Create NFS export directory
    sudo mkdir -p /shared/gridmr/{input,jobs}
    sudo chown -R $USER:$USER /shared/gridmr
    sudo chmod -R 755 /shared/gridmr

    # Configure NFS exports
    echo "/shared/gridmr *(rw,sync,no_subtree_check,no_root_squash)" | sudo tee -a /etc/exports

    # Restart NFS services
    sudo systemctl restart nfs-kernel-server
    sudo systemctl enable nfs-kernel-server

    # Export the filesystem
    sudo exportfs -ra

    print_success "NFS Server configured"
    print_success "Export directory: /shared/gridmr"
    print_success "Subdirectories: /shared/gridmr/input, /shared/gridmr/jobs"

    echo
    echo -e "${YELLOW}Next steps for NFS Server:${NC}"
    echo "1. Add sample data to /shared/gridmr/input/"
    echo "2. Configure AWS Security Group to allow NFS traffic (port 2049)"
    echo "3. Start GridMR master with: python cli.py master --use-nfs"
}

setup_nfs_client() {
    local SERVER_IP=$1

    if [ -z "$SERVER_IP" ]; then
        echo "Usage: $0 client <NFS_SERVER_IP>"
        exit 1
    fi

    echo -e "${BLUE}Setting up NFS Client (Worker/Client Node)...${NC}"

    # Install NFS client
    sudo apt-get update
    sudo apt-get install -y nfs-common

    # Create mount point
    sudo mkdir -p /mnt/gridmr

    # Mount NFS share
    sudo mount -t nfs ${SERVER_IP}:/shared/gridmr /mnt/gridmr

    # Add to fstab for permanent mounting
    echo "${SERVER_IP}:/shared/gridmr /mnt/gridmr nfs defaults 0 0" | sudo tee -a /etc/fstab

    # Set permissions
    sudo chown -R $USER:$USER /mnt/gridmr

    print_success "NFS Client configured"
    print_success "Mount point: /mnt/gridmr"
    print_success "Server: ${SERVER_IP}:/shared/gridmr"

    echo
    echo -e "${YELLOW}Next steps for NFS Client:${NC}"
    echo "1. Verify mount: ls -la /mnt/gridmr"
    echo "2. Start GridMR worker with: python cli.py worker <master_ip> 8000 --use-nfs"
    echo "3. Submit jobs with: python cli.py client <master_ip> file:///mnt/gridmr/input/data wordcount --use-nfs"
}

create_sample_data() {
    local INPUT_DIR="/shared/gridmr/input"

    echo -e "${BLUE}Creating sample data for testing...${NC}"

    mkdir -p "$INPUT_DIR/sample"

    # Create sample text files
    cat > "$INPUT_DIR/sample/shakespeare.txt" << 'EOF'
To be or not to be, that is the question:
Whether 'tis nobler in the mind to suffer
The slings and arrows of outrageous fortune,
Or to take arms against a sea of troubles
And by opposing end them.
EOF

    cat > "$INPUT_DIR/sample/declaration.txt" << 'EOF'
We hold these truths to be self-evident, that all men are created equal,
that they are endowed by their Creator with certain unalienable Rights,
that among these are Life, Liberty and the pursuit of Happiness.
EOF

    cat > "$INPUT_DIR/sample/gettysburg.txt" << 'EOF'
Four score and seven years ago our fathers brought forth on this continent,
a new nation, conceived in Liberty, and dedicated to the proposition that
all men are created equal.
EOF

    chown -R $USER:$USER "$INPUT_DIR"
    chmod -R 644 "$INPUT_DIR"/*

    print_success "Sample data created in $INPUT_DIR/sample/"
    print_success "Files: shakespeare.txt, declaration.txt, gettysburg.txt"
}

show_usage() {
    echo "Usage: $0 [server|client|sample] [options]"
    echo
    echo "Commands:"
    echo "  server          Setup NFS server (for master node)"
    echo "  client <ip>     Setup NFS client (for worker/client nodes)"
    echo "  sample          Create sample input data"
    echo
    echo "Examples:"
    echo "  # On master node (NFS server):"
    echo "  $0 server"
    echo "  $0 sample"
    echo
    echo "  # On worker/client nodes:"
    echo "  $0 client 192.168.1.100"
    echo
    echo "AWS Security Group Requirements:"
    echo "  - Port 2049 (NFS) open between all nodes"
    echo "  - Port 8000 (Master API) open from clients"
    echo "  - Port 8001+ (Worker APIs) open from master"
}

check_root() {
    if [ "$EUID" -eq 0 ]; then
        print_error "Don't run this script as root. Use sudo when prompted."
        exit 1
    fi
}

# Main script
main() {
    print_header
    check_root

    case "${1:-}" in
        "server")
            setup_nfs_server
            ;;
        "client")
            setup_nfs_client "$2"
            ;;
        "sample")
            create_sample_data
            ;;
        *)
            show_usage
            exit 1
            ;;
    esac
}

main "$@"
