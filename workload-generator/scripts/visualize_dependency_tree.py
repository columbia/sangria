#!/usr/bin/env python3
"""
Visualize the transaction dependency tree from the resolver.

This script:
1. Connects to the resolver gRPC service
2. Fetches the dependency tree
3. Creates an HTML visualization showing:
   - Green nodes: committed transactions
   - Red nodes: aborted transactions
   - Arrows showing dependencies (A→B means B depends on A)
   - Cascading aborts visible through the tree structure

=== HOW TO USE ===
1. Run an experiment with Pipelined baseline and abort rate > 0
2. After the experiment, run this script:
   python3 visualize_dependency_tree.py --output tree.html

3. Open tree.html in a browser to see the visualization
"""

import argparse
import json
import sys
from pathlib import Path

# Try to import grpc, fall back to reading from JSON file
try:
    import grpc
    from google.protobuf import descriptor_pb2
    GRPC_AVAILABLE = True
except ImportError:
    GRPC_AVAILABLE = False
    print("Warning: grpc not available, will need JSON input file")


def create_html_visualization(transactions: list, num_committed: int, num_aborted: int, output_path: str):
    """Create an interactive HTML visualization of the dependency tree."""

    # Build nodes and edges for vis.js
    nodes = []
    edges = []

    for tx in transactions:
        tx_id = tx['id']
        status = tx['status']
        short_id = tx_id[:8]

        # Color based on status
        if status == "committed":
            color = "#4CAF50"  # green
            border_color = "#2E7D32"
        elif status == "aborted":
            color = "#f44336"  # red
            border_color = "#c62828"
        else:  # pending
            color = "#9E9E9E"  # gray
            border_color = "#616161"

        nodes.append({
            "id": tx_id,
            "label": f"{short_id}\\n({status})",
            "color": {"background": color, "border": border_color},
            "status": status,
            "dependencies": tx.get('dependencies', []),
            "dependents": tx.get('dependents', [])
        })

        # Add edges for dependencies (dependency → this transaction)
        for dep_id in tx.get('dependencies', []):
            edges.append({
                "from": dep_id,
                "to": tx_id,
                "arrows": "to",
                "color": {"color": "#666", "highlight": "#333"}
            })

    html_content = f"""<!DOCTYPE html>
<html>
<head>
    <title>Transaction Dependency Tree</title>
    <script src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 20px;
            background: #f5f5f5;
        }}
        h1 {{
            color: #333;
            margin-bottom: 10px;
        }}
        .container {{
            display: flex;
            gap: 20px;
        }}
        #network {{
            width: 70%;
            height: 700px;
            border: 1px solid #ccc;
            background: white;
        }}
        .sidebar {{
            width: 30%;
        }}
        .stats {{
            background: white;
            padding: 15px;
            border-radius: 5px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .stats h3 {{
            margin-top: 0;
            color: #333;
        }}
        .stat-row {{
            display: flex;
            justify-content: space-between;
            padding: 5px 0;
            border-bottom: 1px solid #eee;
        }}
        .stat-label {{
            color: #666;
        }}
        .stat-value {{
            font-weight: bold;
        }}
        .committed {{ color: #4CAF50; }}
        .aborted {{ color: #f44336; }}
        .legend {{
            background: white;
            padding: 15px;
            border-radius: 5px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .legend h3 {{
            margin-top: 0;
        }}
        .legend-item {{
            display: flex;
            align-items: center;
            gap: 10px;
            padding: 5px 0;
        }}
        .legend-color {{
            width: 20px;
            height: 20px;
            border-radius: 3px;
        }}
        .info-panel {{
            background: white;
            padding: 15px;
            border-radius: 5px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .info-panel h3 {{
            margin-top: 0;
        }}
        #node-details {{
            font-size: 14px;
        }}
        .detail-label {{
            color: #666;
            font-size: 12px;
        }}
        .detail-value {{
            margin-bottom: 10px;
            word-break: break-all;
        }}
    </style>
</head>
<body>
    <h1>Transaction Dependency Tree (Pipelined 2PC)</h1>
    <p>Arrows show dependencies: A → B means transaction B depends on A (B read uncommitted data from A)</p>

    <div class="container">
        <div id="network"></div>
        <div class="sidebar">
            <div class="stats">
                <h3>Summary</h3>
                <div class="stat-row">
                    <span class="stat-label">Total Transactions</span>
                    <span class="stat-value">{len(transactions)}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">Committed</span>
                    <span class="stat-value committed">{num_committed}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">Aborted</span>
                    <span class="stat-value aborted">{num_aborted}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">Abort Rate</span>
                    <span class="stat-value">{num_aborted / max(len(transactions), 1) * 100:.1f}%</span>
                </div>
            </div>

            <div class="legend">
                <h3>Legend</h3>
                <div class="legend-item">
                    <div class="legend-color" style="background: #4CAF50;"></div>
                    <span>Committed</span>
                </div>
                <div class="legend-item">
                    <div class="legend-color" style="background: #f44336;"></div>
                    <span>Aborted (cascading or artificial)</span>
                </div>
                <div class="legend-item">
                    <div class="legend-color" style="background: #9E9E9E;"></div>
                    <span>Pending</span>
                </div>
                <div class="legend-item">
                    <span>→</span>
                    <span>Dependency edge (A→B: B depends on A)</span>
                </div>
            </div>

            <div class="info-panel">
                <h3>Transaction Details</h3>
                <div id="node-details">
                    <p style="color: #666;">Click on a node to see details</p>
                </div>
            </div>
        </div>
    </div>

    <script>
        var nodes = new vis.DataSet({json.dumps(nodes)});
        var edges = new vis.DataSet({json.dumps(edges)});

        var container = document.getElementById('network');
        var data = {{ nodes: nodes, edges: edges }};
        var options = {{
            layout: {{
                hierarchical: {{
                    enabled: true,
                    direction: 'UD',
                    sortMethod: 'directed',
                    levelSeparation: 80,
                    nodeSpacing: 120,
                    treeSpacing: 200
                }}
            }},
            physics: {{
                enabled: false
            }},
            nodes: {{
                shape: 'box',
                font: {{ size: 11, face: 'monospace' }},
                margin: 8,
                borderWidth: 2
            }},
            edges: {{
                smooth: {{ type: 'cubicBezier', roundness: 0.5 }},
                width: 1.5
            }},
            interaction: {{
                hover: true,
                tooltipDelay: 100
            }}
        }};

        var network = new vis.Network(container, data, options);

        network.on("click", function(params) {{
            if (params.nodes.length > 0) {{
                var nodeId = params.nodes[0];
                var node = nodes.get(nodeId);
                var details = document.getElementById('node-details');

                var depsHtml = node.dependencies.length > 0
                    ? node.dependencies.map(d => '<code>' + d.substr(0,8) + '...</code>').join(', ')
                    : '<span style="color:#999">None</span>';
                var dependentsHtml = node.dependents.length > 0
                    ? node.dependents.map(d => '<code>' + d.substr(0,8) + '...</code>').join(', ')
                    : '<span style="color:#999">None</span>';

                details.innerHTML =
                    '<div class="detail-label">Transaction ID</div>' +
                    '<div class="detail-value"><code>' + nodeId + '</code></div>' +
                    '<div class="detail-label">Status</div>' +
                    '<div class="detail-value" style="color:' + (node.status === 'committed' ? '#4CAF50' : '#f44336') + '"><strong>' + node.status.toUpperCase() + '</strong></div>' +
                    '<div class="detail-label">Depends On (' + node.dependencies.length + ')</div>' +
                    '<div class="detail-value">' + depsHtml + '</div>' +
                    '<div class="detail-label">Dependents (' + node.dependents.length + ')</div>' +
                    '<div class="detail-value">' + dependentsHtml + '</div>';
            }}
        }});

        // Highlight cascade paths on hover
        network.on("hoverNode", function(params) {{
            var nodeId = params.node;
            var node = nodes.get(nodeId);

            // Highlight all nodes in the dependency chain
            if (node.status === 'aborted') {{
                var toHighlight = new Set([nodeId]);
                // Find all dependents (downstream cascade)
                var queue = [nodeId];
                while (queue.length > 0) {{
                    var current = queue.shift();
                    var currentNode = nodes.get(current);
                    if (currentNode && currentNode.dependents) {{
                        currentNode.dependents.forEach(function(dep) {{
                            if (!toHighlight.has(dep)) {{
                                toHighlight.add(dep);
                                queue.push(dep);
                            }}
                        }});
                    }}
                }}
            }}
        }});
    </script>
</body>
</html>
"""

    with open(output_path, 'w') as f:
        f.write(html_content)
    print(f"Saved visualization to: {output_path}")


def fetch_from_grpc(resolver_addr: str) -> dict:
    """Fetch dependency tree from resolver via gRPC."""
    if not GRPC_AVAILABLE:
        raise RuntimeError("grpc not available")

    # This would require generated proto stubs - for now, use JSON fallback
    raise NotImplementedError("gRPC client not implemented - use --json-file instead")


def main():
    parser = argparse.ArgumentParser(description="Visualize transaction dependency tree")
    parser.add_argument("--resolver-addr", type=str, default="localhost:50052",
                        help="Resolver gRPC address")
    parser.add_argument("--json-file", type=str,
                        help="Load dependency tree from JSON file instead of gRPC")
    parser.add_argument("--output", type=str, default="dependency_tree.html",
                        help="Output HTML file path")
    args = parser.parse_args()

    if args.json_file:
        print(f"Loading from JSON file: {args.json_file}")
        with open(args.json_file, 'r') as f:
            data = json.load(f)
        transactions = data.get('transactions', [])
        num_committed = data.get('num_committed', 0)
        num_aborted = data.get('num_aborted', 0)
    else:
        print(f"Fetching from resolver at: {args.resolver_addr}")
        try:
            data = fetch_from_grpc(args.resolver_addr)
            transactions = data.get('transactions', [])
            num_committed = data.get('num_committed', 0)
            num_aborted = data.get('num_aborted', 0)
        except NotImplementedError:
            print("Error: gRPC client not available. Please use --json-file option.")
            print("Run the experiment with the updated code, then save the tree to JSON.")
            sys.exit(1)

    if not transactions:
        print("No transactions found!")
        sys.exit(1)

    print(f"Found {len(transactions)} transactions: {num_committed} committed, {num_aborted} aborted")
    create_html_visualization(transactions, num_committed, num_aborted, args.output)


if __name__ == "__main__":
    main()
