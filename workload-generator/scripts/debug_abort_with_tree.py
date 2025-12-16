#!/usr/bin/env python3
"""
Debug abort experiment that captures server logs to build dependency trees.

This script:
1. Runs a workload with artificial aborts
2. Captures resolver and range server logs
3. Parses dependency relationships and abort cascades
4. Creates a dependency tree visualization

=== HOW TO REPRODUCE ===
1. Run on CloudLab:
   cd ~/sangria
   python3 workload-generator/scripts/debug_abort_with_tree.py --baseline pipelined

2. Results and dependency tree saved to ~/sangria/debug_output/

3. View the tree visualization in dependency_tree.html
"""

import json
import os
import subprocess
import sys
import re
import time
import argparse
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, List, Set, Optional
from collections import defaultdict

# Add parent directory for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

@dataclass
class Transaction:
    id: str
    status: str = "unknown"  # committed, aborted, pending
    dependencies: List[str] = field(default_factory=list)  # txs this one depends on
    dependents: List[str] = field(default_factory=list)  # txs that depend on this one
    abort_reason: Optional[str] = None
    cascaded_from: Optional[str] = None  # if aborted due to cascade, which tx triggered it

@dataclass
class DependencyGraph:
    transactions: Dict[str, Transaction] = field(default_factory=dict)
    cascading_aborts: List[dict] = field(default_factory=list)

    def add_transaction(self, tx_id: str) -> Transaction:
        if tx_id not in self.transactions:
            self.transactions[tx_id] = Transaction(id=tx_id)
        return self.transactions[tx_id]

    def add_dependency(self, tx_id: str, depends_on: str):
        tx = self.add_transaction(tx_id)
        dep = self.add_transaction(depends_on)
        if depends_on not in tx.dependencies:
            tx.dependencies.append(depends_on)
        if tx_id not in dep.dependents:
            dep.dependents.append(tx_id)

    def mark_committed(self, tx_id: str):
        tx = self.add_transaction(tx_id)
        tx.status = "committed"

    def mark_aborted(self, tx_id: str, reason: str = None, cascaded_from: str = None):
        tx = self.add_transaction(tx_id)
        tx.status = "aborted"
        tx.abort_reason = reason
        tx.cascaded_from = cascaded_from


def parse_server_logs(log_content: str, baseline: str) -> DependencyGraph:
    """Parse resolver and range server logs to build dependency graph."""
    graph = DependencyGraph()

    for line in log_content.split('\n'):
        # Parse dependency registration
        # "Dependencies for transaction X: [Y, Z]"
        match = re.search(r'Dependencies for transaction ([a-f0-9-]+): \[([^\]]*)\]', line)
        if match:
            tx_id = match.group(1)
            deps_str = match.group(2)
            if deps_str.strip():
                for dep in re.findall(r'([a-f0-9-]+)', deps_str):
                    graph.add_dependency(tx_id, dep)

        # "Updating dependencies for transaction X"
        match = re.search(r'Updating dependencies for transaction ([a-f0-9-]+)', line)
        if match:
            graph.add_transaction(match.group(1))

        # Parse commits
        # "Transaction X finally committed!"
        match = re.search(r'Transaction ([a-f0-9-]+) finally committed', line)
        if match:
            graph.mark_committed(match.group(1))

        # Parse aborts
        # "Transaction X was aborted"
        match = re.search(r'Transaction ([a-f0-9-]+) was aborted', line)
        if match:
            graph.mark_aborted(match.group(1))

        # "Artificially aborting transaction X"
        match = re.search(r'Artificially aborting transaction ([a-f0-9-]+)', line)
        if match:
            graph.mark_aborted(match.group(1), reason="artificial")

        # Parse cascading aborts (Pipelined only)
        # "Transaction X cascading abort to N dependents: [Y, Z]"
        match = re.search(r'Transaction ([a-f0-9-]+) cascading abort to (\d+) dependents: \[([^\]]*)\]', line)
        if match:
            source_tx = match.group(1)
            num_deps = int(match.group(2))
            deps_str = match.group(3)
            dependents = re.findall(r'([a-f0-9-]+)', deps_str)

            graph.cascading_aborts.append({
                "source": source_tx,
                "num_dependents": num_deps,
                "dependents": dependents
            })

            for dep_tx in dependents:
                graph.mark_aborted(dep_tx, reason="cascading", cascaded_from=source_tx)

        # "Dependency X was aborted, transaction Y must also abort"
        match = re.search(r'Dependency ([a-f0-9-]+) was aborted.*transaction ([a-f0-9-]+) must also abort', line)
        if match:
            dep_tx = match.group(1)
            tx_id = match.group(2)
            graph.mark_aborted(tx_id, reason="dependency_aborted", cascaded_from=dep_tx)

        # "Cascading abort to dependent transaction X"
        match = re.search(r'Cascading abort to dependent transaction ([a-f0-9-]+)', line)
        if match:
            graph.mark_aborted(match.group(1), reason="cascading")

    return graph


def create_tree_html(graph: DependencyGraph, baseline: str, output_path: str):
    """Create an interactive HTML visualization of the dependency tree."""

    # Build tree data for visualization
    nodes = []
    edges = []

    for tx_id, tx in graph.transactions.items():
        short_id = tx_id[:8]

        # Color based on status
        if tx.status == "committed":
            color = "#4CAF50"  # green
        elif tx.status == "aborted":
            if tx.abort_reason == "artificial":
                color = "#f44336"  # red - root cause
            elif tx.abort_reason == "cascading":
                color = "#FF9800"  # orange - cascading
            else:
                color = "#E91E63"  # pink - other abort
        else:
            color = "#9E9E9E"  # gray - unknown

        label = f"{short_id}"
        if tx.status == "aborted" and tx.abort_reason:
            label += f"\n({tx.abort_reason})"

        nodes.append({
            "id": tx_id,
            "label": label,
            "color": color,
            "status": tx.status,
            "reason": tx.abort_reason,
            "cascaded_from": tx.cascaded_from,
            "dependencies": tx.dependencies,
            "dependents": tx.dependents
        })

        # Add edges for dependencies
        for dep in tx.dependencies:
            edges.append({
                "from": dep,
                "to": tx_id,
                "arrows": "to",
                "color": "#666"
            })

    # Calculate stats
    committed = sum(1 for tx in graph.transactions.values() if tx.status == "committed")
    aborted = sum(1 for tx in graph.transactions.values() if tx.status == "aborted")
    artificial = sum(1 for tx in graph.transactions.values() if tx.abort_reason == "artificial")
    cascading = sum(1 for tx in graph.transactions.values() if tx.abort_reason in ["cascading", "dependency_aborted"])

    html_content = f"""<!DOCTYPE html>
<html>
<head>
    <title>Transaction Dependency Tree - {baseline}</title>
    <script src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        #network {{ width: 100%; height: 600px; border: 1px solid #ccc; }}
        .stats {{ background: #f5f5f5; padding: 15px; margin-bottom: 20px; border-radius: 5px; }}
        .legend {{ display: flex; gap: 20px; margin-bottom: 20px; }}
        .legend-item {{ display: flex; align-items: center; gap: 5px; }}
        .legend-color {{ width: 20px; height: 20px; border-radius: 3px; }}
        .info-panel {{ background: #fff; padding: 15px; border: 1px solid #ddd; margin-top: 20px; }}
        h1 {{ color: #333; }}
        .cascade-chain {{ background: #fff3e0; padding: 10px; margin: 10px 0; border-left: 4px solid #ff9800; }}
    </style>
</head>
<body>
    <h1>Transaction Dependency Tree - {baseline}</h1>

    <div class="stats">
        <h3>Summary</h3>
        <p><strong>Total Transactions:</strong> {len(graph.transactions)}</p>
        <p><strong>Committed:</strong> <span style="color: #4CAF50;">{committed}</span></p>
        <p><strong>Aborted:</strong> <span style="color: #f44336;">{aborted}</span></p>
        <p><strong>├─ Artificial (root cause):</strong> {artificial}</p>
        <p><strong>└─ Cascading:</strong> {cascading}</p>
        <p><strong>Cascading Abort Events:</strong> {len(graph.cascading_aborts)}</p>
    </div>

    <div class="legend">
        <div class="legend-item"><div class="legend-color" style="background: #4CAF50;"></div> Committed</div>
        <div class="legend-item"><div class="legend-color" style="background: #f44336;"></div> Aborted (artificial/root)</div>
        <div class="legend-item"><div class="legend-color" style="background: #FF9800;"></div> Aborted (cascading)</div>
        <div class="legend-item"><div class="legend-color" style="background: #9E9E9E;"></div> Unknown</div>
        <div class="legend-item">→ Dependency (A→B means B depends on A)</div>
    </div>

    <div id="network"></div>

    <div class="info-panel">
        <h3>Cascading Abort Chains</h3>
        {"".join([f'''<div class="cascade-chain">
            <strong>Source:</strong> {ca["source"][:8]}... →
            <strong>{ca["num_dependents"]} dependents:</strong> {", ".join([d[:8]+"..." for d in ca["dependents"]])}
        </div>''' for ca in graph.cascading_aborts]) or "<p>No cascading aborts detected</p>"}
    </div>

    <div class="info-panel" id="node-details">
        <h3>Transaction Details</h3>
        <p>Click on a node to see details</p>
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
                    levelSeparation: 100,
                    nodeSpacing: 150
                }}
            }},
            physics: {{
                enabled: false
            }},
            nodes: {{
                shape: 'box',
                font: {{ size: 12 }},
                margin: 10
            }},
            edges: {{
                smooth: {{ type: 'cubicBezier' }}
            }}
        }};

        var network = new vis.Network(container, data, options);

        network.on("click", function(params) {{
            if (params.nodes.length > 0) {{
                var nodeId = params.nodes[0];
                var node = nodes.get(nodeId);
                var details = document.getElementById('node-details');
                details.innerHTML = '<h3>Transaction Details</h3>' +
                    '<p><strong>ID:</strong> ' + nodeId + '</p>' +
                    '<p><strong>Status:</strong> ' + node.status + '</p>' +
                    (node.reason ? '<p><strong>Abort Reason:</strong> ' + node.reason + '</p>' : '') +
                    (node.cascaded_from ? '<p><strong>Cascaded From:</strong> ' + node.cascaded_from + '</p>' : '') +
                    '<p><strong>Dependencies:</strong> ' + (node.dependencies.length > 0 ? node.dependencies.map(d => d.substr(0,8)+'...').join(', ') : 'None') + '</p>' +
                    '<p><strong>Dependents:</strong> ' + (node.dependents.length > 0 ? node.dependents.map(d => d.substr(0,8)+'...').join(', ') : 'None') + '</p>';
            }}
        }});
    </script>
</body>
</html>
"""

    with open(output_path, 'w') as f:
        f.write(html_content)
    print(f"Saved: {output_path}")


def create_text_tree(graph: DependencyGraph) -> str:
    """Create a text-based tree representation."""
    lines = []
    lines.append("=" * 60)
    lines.append("DEPENDENCY TREE")
    lines.append("=" * 60)

    # Find root transactions (no dependencies)
    roots = [tx_id for tx_id, tx in graph.transactions.items() if not tx.dependencies]

    def print_tree(tx_id: str, prefix: str = "", is_last: bool = True, visited: set = None):
        if visited is None:
            visited = set()
        if tx_id in visited:
            return [f"{prefix}{'└── ' if is_last else '├── '}[CYCLE: {tx_id[:8]}...]"]
        visited.add(tx_id)

        tx = graph.transactions.get(tx_id)
        if not tx:
            return []

        status_icon = "✓" if tx.status == "committed" else "✗" if tx.status == "aborted" else "?"
        reason = f" ({tx.abort_reason})" if tx.abort_reason else ""
        cascade = f" ← from {tx.cascaded_from[:8]}..." if tx.cascaded_from else ""

        result = [f"{prefix}{'└── ' if is_last else '├── '}{status_icon} {tx_id[:8]}...{reason}{cascade}"]

        child_prefix = prefix + ("    " if is_last else "│   ")
        dependents = tx.dependents
        for i, dep_id in enumerate(dependents):
            is_last_child = (i == len(dependents) - 1)
            result.extend(print_tree(dep_id, child_prefix, is_last_child, visited.copy()))

        return result

    if roots:
        lines.append("\nDependency chains (root → dependents):\n")
        for i, root_id in enumerate(roots):
            lines.extend(print_tree(root_id, "", i == len(roots) - 1))
    else:
        lines.append("\nNo root transactions found (all have dependencies)")

    # Summary
    lines.append("\n" + "=" * 60)
    lines.append("SUMMARY")
    lines.append("=" * 60)

    committed = [tx for tx in graph.transactions.values() if tx.status == "committed"]
    aborted = [tx for tx in graph.transactions.values() if tx.status == "aborted"]
    artificial = [tx for tx in aborted if tx.abort_reason == "artificial"]
    cascading = [tx for tx in aborted if tx.abort_reason in ["cascading", "dependency_aborted"]]

    lines.append(f"Total transactions: {len(graph.transactions)}")
    lines.append(f"Committed: {len(committed)}")
    lines.append(f"Aborted: {len(aborted)}")
    lines.append(f"  - Artificial (root cause): {len(artificial)}")
    lines.append(f"  - Cascading: {len(cascading)}")

    if graph.cascading_aborts:
        lines.append(f"\nCascading abort events: {len(graph.cascading_aborts)}")
        for ca in graph.cascading_aborts:
            lines.append(f"  {ca['source'][:8]}... → {ca['num_dependents']} dependents")

    return "\n".join(lines)


def run_experiment(baseline: str, output_dir: Path, duration_seconds: int = 30):
    """Run the experiment and capture server logs."""

    print(f"Running {baseline} experiment for {duration_seconds}s...")

    # Paths
    sangria_dir = Path.home() / "sangria"
    config_path = sangria_dir / "config" / "servers_config.json"
    workload_config_path = output_dir / "workload_config.json"

    # Load and modify config
    with open(config_path, 'r') as f:
        config = json.load(f)

    config["commit_strategy"] = baseline
    config["range_server"]["artificial_abort_rate"] = 0.3

    with open(config_path, 'w') as f:
        json.dump(config, f, indent=2)

    # Workload config
    workload_config = {
        "num_keys": 10,
        "max_concurrency": "15",
        "num_queries": 200,
        "zipf_exponent": 0.9,
        "namespace": "debug",
        "name": "tree",
        "background_runtime_core_ids": list(range(3, 32)),
        "workload_type": "dependency",
        "fake_transactions": False
    }

    with open(workload_config_path, 'w') as f:
        json.dump(workload_config, f, indent=2)

    # Kill existing servers
    subprocess.run(["pkill", "-f", "range-server"], capture_output=True)
    subprocess.run(["pkill", "-f", "resolver"], capture_output=True)
    time.sleep(2)

    # Start servers with logging
    server_log_path = output_dir / f"{baseline}_server.log"

    env = {**os.environ, "RUST_LOG": "info"}

    # Start range server
    range_server_cmd = [
        str(sangria_dir / "target" / "release" / "range-server"),
        "--config", str(config_path)
    ]

    resolver_cmd = [
        str(sangria_dir / "target" / "release" / "resolver"),
        "--config", str(config_path)
    ]

    print("Starting servers...")
    with open(server_log_path, 'w') as log_file:
        range_server_proc = subprocess.Popen(
            range_server_cmd,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            env=env,
            cwd=sangria_dir
        )

        resolver_proc = subprocess.Popen(
            resolver_cmd,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            env=env,
            cwd=sangria_dir
        )

        time.sleep(5)  # Wait for servers to start

        # Run workload
        print("Running workload...")
        workload_cmd = [
            str(sangria_dir / "target" / "release" / "workload-generator"),
            "--workload-config", str(workload_config_path),
            "--config", str(config_path),
            "--create-keyspace"
        ]

        workload_proc = subprocess.Popen(
            workload_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
            cwd=sangria_dir / "workload-generator"
        )

        try:
            stdout, stderr = workload_proc.communicate(timeout=120)
            print("Workload completed")
            print(stdout)
        except subprocess.TimeoutExpired:
            workload_proc.kill()
            print("Workload timed out")

        # Stop servers
        print("Stopping servers...")
        range_server_proc.terminate()
        resolver_proc.terminate()
        time.sleep(2)

    return server_log_path


def main():
    parser = argparse.ArgumentParser(description="Debug abort experiment with dependency tree visualization")
    parser.add_argument("--baseline", choices=["Pipelined", "Traditional", "pipelined", "traditional"],
                        default="Pipelined", help="Baseline to test")
    parser.add_argument("--output-dir", type=str, default=str(Path.home() / "sangria" / "debug_output"),
                        help="Output directory")
    parser.add_argument("--log-file", type=str, help="Parse existing log file instead of running experiment")
    args = parser.parse_args()

    baseline = args.baseline.capitalize() if args.baseline else "Pipelined"
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.log_file:
        # Parse existing log file
        log_path = Path(args.log_file)
        print(f"Parsing existing log file: {log_path}")
        with open(log_path, 'r') as f:
            log_content = f.read()
    else:
        # Run experiment
        log_path = run_experiment(baseline, output_dir)
        with open(log_path, 'r') as f:
            log_content = f.read()

    # Parse logs
    print("Parsing server logs...")
    graph = parse_server_logs(log_content, baseline)

    # Print text tree
    text_tree = create_text_tree(graph)
    print(text_tree)

    # Save text tree
    text_path = output_dir / f"{baseline.lower()}_dependency_tree.txt"
    with open(text_path, 'w') as f:
        f.write(text_tree)
    print(f"Saved: {text_path}")

    # Create HTML visualization
    html_path = output_dir / f"{baseline.lower()}_dependency_tree.html"
    create_tree_html(graph, baseline, str(html_path))

    # Save graph data as JSON
    json_path = output_dir / f"{baseline.lower()}_dependency_graph.json"
    graph_data = {
        "baseline": baseline,
        "transactions": {
            tx_id: {
                "status": tx.status,
                "dependencies": tx.dependencies,
                "dependents": tx.dependents,
                "abort_reason": tx.abort_reason,
                "cascaded_from": tx.cascaded_from
            }
            for tx_id, tx in graph.transactions.items()
        },
        "cascading_aborts": graph.cascading_aborts,
        "summary": {
            "total": len(graph.transactions),
            "committed": sum(1 for tx in graph.transactions.values() if tx.status == "committed"),
            "aborted": sum(1 for tx in graph.transactions.values() if tx.status == "aborted"),
            "artificial_aborts": sum(1 for tx in graph.transactions.values() if tx.abort_reason == "artificial"),
            "cascading_aborts": sum(1 for tx in graph.transactions.values() if tx.abort_reason in ["cascading", "dependency_aborted"])
        }
    }
    with open(json_path, 'w') as f:
        json.dump(graph_data, f, indent=2)
    print(f"Saved: {json_path}")


if __name__ == "__main__":
    main()
