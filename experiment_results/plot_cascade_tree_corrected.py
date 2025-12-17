#!/usr/bin/env python3
"""
CORRECTED cascade tree visualization.
Edge format is [child, parent] (child depends on parent).

Categories:
  - Green: Committed
  - Red: Artificially Aborted (root cause - no aborted parent)
  - Orange: Cascadingly Aborted (has aborted parent)
  - Gray: Unknown (has parent not in dataset)
"""

import json
import plotly.graph_objects as go
from collections import defaultdict

# Load data
data_path = "/Users/ifesionubogu/Desktop/shared_sangria_2/sangria/experiment_results/cascade_data_zipf0.1_abort10pct.json"

with open(data_path) as f:
    data = json.load(f)

nodes_list = data.get('nodes', [])
edges_list = data.get('edges', [])

print(f"Total nodes: {len(nodes_list)}")
print(f"Total edges: {len(edges_list)}")

# Build node dict and CORRECTED parent/child mappings
# CORRECTED: edge = [child, parent] meaning child depends on parent
nodes = {n['id']: n['status'] for n in nodes_list}
parents = defaultdict(list)   # parents[node] = list of parent nodes
children = defaultdict(list)  # children[node] = list of child nodes

for edge in edges_list:
    child_id, parent_id = edge[0], edge[1]  # CORRECTED order
    parents[child_id].append(parent_id)
    children[parent_id].append(child_id)

# Categorize nodes
categories = {
    'committed': [],
    'artificially_aborted': [],
    'cascadingly_aborted': [],
    'unknown': []
}

for nid, status in nodes.items():
    node_parents = parents.get(nid, [])
    missing_parents = [p for p in node_parents if p not in nodes]

    if missing_parents:
        categories['unknown'].append(nid)
    elif status == 1:
        categories['committed'].append(nid)
    elif status == 0:
        # Check if any parent aborted
        has_aborted_parent = any(nodes.get(p, 1) == 0 for p in node_parents)
        if has_aborted_parent:
            categories['cascadingly_aborted'].append(nid)
        else:
            categories['artificially_aborted'].append(nid)

print(f"\nCategories:")
for cat, ids in categories.items():
    pct = len(ids) / len(nodes) * 100 if nodes else 0
    print(f"  {cat}: {len(ids)} ({pct:.2f}%)")

# Verify invariant
print("\nVerifying cascade invariant...")
violations = 0
for nid, status in nodes.items():
    if status == 1:  # committed
        for parent_id in parents.get(nid, []):
            if parent_id in nodes and nodes[parent_id] == 0:
                violations += 1
print(f"Violations (committed child with aborted parent): {violations}")

# Get color for node
def get_category(nid):
    if nid in categories['artificially_aborted']:
        return 'artificially_aborted'
    elif nid in categories['cascadingly_aborted']:
        return 'cascadingly_aborted'
    elif nid in categories['unknown']:
        return 'unknown'
    else:
        return 'committed'

def get_color(cat):
    return {
        'artificially_aborted': '#e74c3c',  # Red
        'cascadingly_aborted': '#f39c12',   # Orange
        'unknown': '#95a5a6',               # Gray
        'committed': '#2ecc71'              # Green
    }.get(cat, 'gray')

# Build cascade trees from root aborts
# Root aborts propagate DOWN to their children
print("\n" + "="*60)
print("CASCADE CHAINS (Root -> Children)")
print("="*60)

for i, root_id in enumerate(categories['artificially_aborted']):
    print(f"\n--- Chain {i+1}: Root {root_id[:16]}... ---")

    # BFS from root to children
    visited = set([root_id])
    queue = [(root_id, 0)]
    level_data = defaultdict(lambda: {'aborted': [], 'committed': []})

    while queue:
        nid, level = queue.pop(0)
        status = nodes[nid]

        if status == 0:
            level_data[level]['aborted'].append(nid)
        else:
            level_data[level]['committed'].append(nid)

        for child in children.get(nid, []):
            if child in nodes and child not in visited:
                visited.add(child)
                queue.append((child, level + 1))

    for level in sorted(level_data.keys()):
        ld = level_data[level]
        print(f"  Level {level}:")
        if ld['aborted']:
            print(f"    Aborted: {len(ld['aborted'])}")
        if ld['committed']:
            print(f"    Committed: {len(ld['committed'])}")

# Build visualization focused on abort chains only
# Show all aborted nodes and their relationships

nodes_to_show = set()
nodes_to_show.update(categories['artificially_aborted'])
nodes_to_show.update(categories['cascadingly_aborted'])

# Add immediate parents/children of aborted nodes for context
for nid in list(nodes_to_show):
    for p in parents.get(nid, []):
        if p in nodes:
            nodes_to_show.add(p)
    for c in children.get(nid, []):
        if c in nodes:
            nodes_to_show.add(c)

print(f"\nNodes to visualize: {len(nodes_to_show)}")

# Build hierarchical layout
def build_layout(root_nodes, node_set):
    positions = {}
    level_assignments = {}

    # BFS from roots (going DOWN to children)
    queue = [(nid, 0) for nid in root_nodes if nid in node_set]
    visited = set(nid for nid, _ in queue)

    while queue:
        nid, level = queue.pop(0)
        level_assignments[nid] = level

        for child in children.get(nid, []):
            if child in node_set and child not in visited:
                visited.add(child)
                queue.append((child, level + 1))

    # Assign remaining nodes
    for nid in node_set:
        if nid not in level_assignments:
            level_assignments[nid] = 0

    # Group by level and assign x positions
    nodes_by_level = defaultdict(list)
    for nid, level in level_assignments.items():
        nodes_by_level[level].append(nid)

    for level, nodes_at_level in nodes_by_level.items():
        n = len(nodes_at_level)
        for i, nid in enumerate(sorted(nodes_at_level)):
            x = (i - n/2) * 1.5
            y = -level * 2
            positions[nid] = (x, y)

    return positions

# Find root nodes (artificially aborted have no aborted parents)
root_nodes = categories['artificially_aborted']
positions = build_layout(root_nodes, nodes_to_show)

# Create figure
fig = go.Figure()

# Draw edges (parent -> child direction)
edge_x = []
edge_y = []
for nid in nodes_to_show:
    if nid not in positions:
        continue
    x1, y1 = positions[nid]
    for child in children.get(nid, []):
        if child in nodes_to_show and child in positions:
            x2, y2 = positions[child]
            edge_x.extend([x1, x2, None])
            edge_y.extend([y1, y2, None])

fig.add_trace(go.Scatter(
    x=edge_x, y=edge_y,
    mode='lines',
    line=dict(width=1, color='#888'),
    hoverinfo='none',
    name='Dependencies'
))

# Draw nodes by category
category_labels = {
    'artificially_aborted': 'Root Cause Abort',
    'cascadingly_aborted': 'Cascaded Abort',
    'committed': 'Committed',
    'unknown': 'Unknown'
}

for cat in ['artificially_aborted', 'cascadingly_aborted', 'committed', 'unknown']:
    cat_nodes = [nid for nid in nodes_to_show if get_category(nid) == cat and nid in positions]
    if not cat_nodes:
        continue

    xs = [positions[nid][0] for nid in cat_nodes]
    ys = [positions[nid][1] for nid in cat_nodes]
    hovers = [f"TX: {nid[:12]}...<br>Category: {category_labels[cat]}" for nid in cat_nodes]
    labels = [nid[:6] for nid in cat_nodes]

    size = 30 if cat in ['artificially_aborted', 'cascadingly_aborted'] else 18
    symbol = 'star' if cat == 'artificially_aborted' else 'circle'

    fig.add_trace(go.Scatter(
        x=xs, y=ys,
        mode='markers+text',
        marker=dict(
            size=size,
            color=get_color(cat),
            line=dict(width=2, color='white'),
            symbol=symbol
        ),
        text=labels,
        textposition='bottom center',
        textfont=dict(size=8),
        hovertext=hovers,
        hoverinfo='text',
        name=f'{category_labels[cat]} ({len(cat_nodes)})'
    ))

total_aborted = len(categories['artificially_aborted']) + len(categories['cascadingly_aborted'])
amplification = len(categories['cascadingly_aborted']) / len(categories['artificially_aborted']) if categories['artificially_aborted'] else 0

fig.update_layout(
    title=f'Cascade Dependency Tree (Corrected)<br>'
          f'<sub>{len(categories["artificially_aborted"])} root aborts → {len(categories["cascadingly_aborted"])} cascaded ({amplification:.1f}x amplification)</sub>',
    showlegend=True,
    legend=dict(x=0, y=1, bgcolor='rgba(255,255,255,0.9)'),
    hovermode='closest',
    xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
    yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
    height=800,
    width=1200,
    plot_bgcolor='white'
)

fig.add_annotation(
    text="Stars = Root cause aborts | Orange = Cascaded aborts | Green = Survived | Lines = Dependency flow",
    xref="paper", yref="paper",
    x=0.5, y=-0.05,
    showarrow=False,
    font=dict(size=11)
)

output_path = "/Users/ifesionubogu/Desktop/shared_sangria_2/sangria/experiment_results/cascade_tree_corrected.html"
fig.write_html(output_path)
print(f"\nSaved to: {output_path}")

# Summary stats
print("\n" + "="*60)
print("SUMMARY")
print("="*60)
print(f"Total transactions: {len(nodes)}")
print(f"Committed: {len(categories['committed'])} ({100*len(categories['committed'])/len(nodes):.2f}%)")
print(f"Artificially Aborted: {len(categories['artificially_aborted'])}")
print(f"Cascadingly Aborted: {len(categories['cascadingly_aborted'])}")
print(f"Unknown: {len(categories['unknown'])}")
print(f"\nCascade amplification: {amplification:.2f}x")
print(f"Total aborts: {total_aborted}")
if total_aborted > 0:
    print(f"  Root cause: {len(categories['artificially_aborted'])} ({100*len(categories['artificially_aborted'])/total_aborted:.1f}%)")
    print(f"  Cascaded: {len(categories['cascadingly_aborted'])} ({100*len(categories['cascadingly_aborted'])/total_aborted:.1f}%)")
