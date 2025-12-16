#!/usr/bin/env python3
"""
Fetch the dependency tree from the resolver and save it to JSON.

This script:
1. Generates Python gRPC stubs from the proto file
2. Connects to the resolver gRPC service
3. Fetches the dependency tree
4. Saves to JSON file that can be visualized

Usage:
    python3 fetch_dependency_tree.py --resolver-addr localhost:50052 --output tree.json
"""

import argparse
import json
import os
import sys
import subprocess
import tempfile

def generate_grpc_stubs(proto_dir: str, output_dir: str):
    """Generate Python gRPC stubs from the proto file."""
    proto_file = os.path.join(proto_dir, "resolver.proto")

    if not os.path.exists(proto_file):
        raise FileNotFoundError(f"Proto file not found: {proto_file}")

    # Generate Python code
    cmd = [
        sys.executable, "-m", "grpc_tools.protoc",
        f"--proto_path={proto_dir}",
        f"--python_out={output_dir}",
        f"--grpc_python_out={output_dir}",
        proto_file
    ]

    print(f"Generating gRPC stubs: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error generating stubs: {result.stderr}")
        raise RuntimeError("Failed to generate gRPC stubs")

    return output_dir


def fetch_dependency_tree(resolver_addr: str, stubs_dir: str) -> dict:
    """Fetch the dependency tree from the resolver."""
    # Add stubs directory to path
    sys.path.insert(0, stubs_dir)

    import grpc
    import resolver_pb2
    import resolver_pb2_grpc

    # Connect to resolver
    channel = grpc.insecure_channel(resolver_addr)
    stub = resolver_pb2_grpc.ResolverStub(channel)

    # Fetch dependency tree
    request = resolver_pb2.GetDependencyTreeRequest()
    response = stub.GetDependencyTree(request)

    # Convert to dict
    transactions = []
    for tx in response.transactions:
        transactions.append({
            "id": tx.id,
            "status": tx.status,
            "dependencies": list(tx.dependencies),
            "dependents": list(tx.dependents),
        })

    return {
        "transactions": transactions,
        "num_committed": response.num_committed,
        "num_aborted": response.num_aborted,
    }


def main():
    parser = argparse.ArgumentParser(description="Fetch dependency tree from resolver")
    parser.add_argument("--resolver-addr", type=str, default="localhost:50052",
                        help="Resolver gRPC address")
    parser.add_argument("--proto-dir", type=str, default=None,
                        help="Directory containing resolver.proto")
    parser.add_argument("--output", type=str, default="dependency_tree.json",
                        help="Output JSON file path")
    args = parser.parse_args()

    # Find proto directory
    if args.proto_dir:
        proto_dir = args.proto_dir
    else:
        # Try to find it relative to this script
        script_dir = os.path.dirname(os.path.abspath(__file__))
        proto_dir = os.path.join(script_dir, "..", "..", "proto", "src")
        if not os.path.exists(os.path.join(proto_dir, "resolver.proto")):
            proto_dir = os.path.expanduser("~/sangria/proto/src")

    print(f"Using proto directory: {proto_dir}")

    # Generate stubs in temp directory
    with tempfile.TemporaryDirectory() as stubs_dir:
        generate_grpc_stubs(proto_dir, stubs_dir)

        print(f"Connecting to resolver at: {args.resolver_addr}")
        try:
            data = fetch_dependency_tree(args.resolver_addr, stubs_dir)
        except Exception as e:
            print(f"Error fetching dependency tree: {e}")
            sys.exit(1)

    print(f"Found {len(data['transactions'])} transactions:")
    print(f"  - Committed: {data['num_committed']}")
    print(f"  - Aborted: {data['num_aborted']}")

    # Save to JSON
    with open(args.output, 'w') as f:
        json.dump(data, f, indent=2)

    print(f"Saved to: {args.output}")

    # Also print summary of cascading aborts
    aborted_txs = [tx for tx in data['transactions'] if tx['status'] == 'aborted']
    if aborted_txs:
        print("\n=== Aborted Transactions ===")
        for tx in aborted_txs[:10]:  # Show first 10
            deps = len(tx['dependencies'])
            dependents = len(tx['dependents'])
            print(f"  {tx['id'][:8]}... - {deps} deps, {dependents} dependents")
        if len(aborted_txs) > 10:
            print(f"  ... and {len(aborted_txs) - 10} more")


if __name__ == "__main__":
    main()
