#!/bin/bash
# Script to deploy hopsfs-performance-eval project to Kubernetes pod

# Default values
NAMESPACE="test"
POD_NAME=""
CONTAINER_NAME="jupyter"
REMOTE_PATH="/tmp/hopsfs-performance-eval"
LOCAL_PATH="$(cd "$(dirname "$0")" && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored messages
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to show usage
usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Deploy hopsfs-performance-eval project to a Kubernetes pod.

OPTIONS:
    -p, --pod POD_NAME          Pod name (required)
    -c, --container CONTAINER   Container name (required if pod has multiple containers)
    -n, --namespace NAMESPACE   Namespace (default: test)
    -r, --remote-path PATH      Remote path on pod (default: /tmp/hopsfs-performance-eval)
    -h, --help                  Show this help message

EXAMPLES:
    # Deploy to specific pod in default namespace
    $0 --pod jupyter-test--meb10000-5878446446-8nx4w

    # Deploy to specific container in pod
    $0 --pod jupyter-test--meb10000-5878446446-8nx4w --container notebook

    # Deploy to specific pod in custom namespace
    $0 --pod my-pod --namespace my-namespace --container app

    # Deploy to custom remote path
    $0 --pod my-pod --container app --remote-path /home/jovyan/benchmark

EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -p|--pod)
            POD_NAME="$2"
            shift 2
            ;;
        -c|--container)
            CONTAINER_NAME="$2"
            shift 2
            ;;
        -n|--namespace)
            NAMESPACE="$2"
            shift 2
            ;;
        -r|--remote-path)
            REMOTE_PATH="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Check if pod name is provided
if [ -z "$POD_NAME" ]; then
    print_error "Pod name is required!"
    usage
    exit 1
fi

# Check if kubectl is available
if ! command -v kubectl &> /dev/null; then
    print_error "kubectl is not installed or not in PATH"
    exit 1
fi

# Verify pod exists
print_info "Checking if pod exists..."
if ! kubectl get pod "$POD_NAME" -n "$NAMESPACE" &> /dev/null; then
    print_error "Pod '$POD_NAME' not found in namespace '$NAMESPACE'"
    print_info "Available pods in namespace '$NAMESPACE':"
    kubectl get pods -n "$NAMESPACE" | grep -v "NAME"
    exit 1
fi

# Build container flag if container name is provided
CONTAINER_FLAG=""
if [ -n "$CONTAINER_NAME" ]; then
    CONTAINER_FLAG="-c $CONTAINER_NAME"

    # Verify container exists in pod
    print_info "Checking if container exists in pod..."
    if ! kubectl get pod "$POD_NAME" -n "$NAMESPACE" -o jsonpath="{.spec.containers[*].name}" | grep -qw "$CONTAINER_NAME"; then
        print_error "Container '$CONTAINER_NAME' not found in pod '$POD_NAME'"
        print_info "Available containers in pod:"
        kubectl get pod "$POD_NAME" -n "$NAMESPACE" -o jsonpath="{.spec.containers[*].name}" | tr ' ' '\n'
        exit 1
    fi
    print_info "Container found: $CONTAINER_NAME"
else
    # Check if pod has multiple containers
    CONTAINER_COUNT=$(kubectl get pod "$POD_NAME" -n "$NAMESPACE" -o jsonpath="{.spec.containers[*].name}" | wc -w)
    if [ "$CONTAINER_COUNT" -gt 1 ]; then
        print_warn "Pod has multiple containers but no container specified"
        print_info "Available containers:"
        kubectl get pod "$POD_NAME" -n "$NAMESPACE" -o jsonpath="{.spec.containers[*].name}" | tr ' ' '\n'
        print_warn "Using default container (first one)"
    fi
fi

print_info "Pod found: $POD_NAME"
print_info "Namespace: $NAMESPACE"
if [ -n "$CONTAINER_NAME" ]; then
    print_info "Container: $CONTAINER_NAME"
fi
print_info "Local path: $LOCAL_PATH"
print_info "Remote path: $REMOTE_PATH"

# Create a temporary directory with only the files we want to copy
print_info "Preparing files for deployment..."
TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT

# Copy project files to temp directory
cp "$LOCAL_PATH/experiment.py" "$TEMP_DIR/"
cp "$LOCAL_PATH/run_benchmark.py" "$TEMP_DIR/"

# Copy any other Python files if they exist
if [ -f "$LOCAL_PATH/requirements.txt" ]; then
    cp "$LOCAL_PATH/requirements.txt" "$TEMP_DIR/"
fi

if [ -f "$LOCAL_PATH/README.md" ]; then
    cp "$LOCAL_PATH/README.md" "$TEMP_DIR/"
fi

print_info "Files to be copied:"
ls -lh "$TEMP_DIR/"

# Remove existing directory on pod (if exists)
print_info "Cleaning up existing directory on pod..."
kubectl exec -n "$NAMESPACE" "$POD_NAME" $CONTAINER_FLAG -- rm -rf "$REMOTE_PATH" 2>/dev/null || true

# Copy files to pod
print_info "Copying files to pod..."
PARENT_DIR=$(dirname "$REMOTE_PATH")
TEMP_DIR_NAME=$(basename "$TEMP_DIR")
KUBECTL_CP_CMD="kubectl cp \"$TEMP_DIR\" \"$NAMESPACE/$POD_NAME:$PARENT_DIR\" --no-preserve=true"
if [ -n "$CONTAINER_NAME" ]; then
    KUBECTL_CP_CMD="$KUBECTL_CP_CMD -c $CONTAINER_NAME"
fi

if eval $KUBECTL_CP_CMD; then
    # Rename the temp directory to the desired name
    kubectl exec -n "$NAMESPACE" "$POD_NAME" $CONTAINER_FLAG -- mv "$PARENT_DIR/$TEMP_DIR_NAME" "$REMOTE_PATH"

    print_info "Files copied successfully!"

    # Make scripts executable
    print_info "Making scripts executable..."
    kubectl exec -n "$NAMESPACE" "$POD_NAME" $CONTAINER_FLAG -- chmod +x "$REMOTE_PATH/run_benchmark.py"

    # List files on pod
    print_info "Files on pod:"
    kubectl exec -n "$NAMESPACE" "$POD_NAME" $CONTAINER_FLAG -- ls -lh "$REMOTE_PATH"

    print_info "Deployment complete!"
    print_info "To run benchmarks, execute:"
    echo ""
    echo "  kubectl exec -it -n $NAMESPACE $POD_NAME $CONTAINER_FLAG -- python3 $REMOTE_PATH/run_benchmark.py --help"
    echo ""
else
    print_error "Failed to copy files to pod"
    exit 1
fi
