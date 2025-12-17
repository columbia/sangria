#!/usr/bin/env python3
"""
Verify cascade logic: if a parent aborts, ALL children must also abort.
A child cannot commit if its parent aborted.

This script checks for any violations of this invariant in the data.
"""

import json
from collections import defaultdict

# Load data
data_path = "/Users/ifesionubogu/Desktop/shared_sangria_2/sangria/experiment_results/cascade_data_zipf0.1_abort10pct.json"

with open(data_path) as f:
    data = json.load(f)

nodes_list = data.get('nodes', [])
edges_list = data.get('edges', [])

print(f"Total nodes: {len(nodes_list)}")
print(f"Total edges: {len(edges_list)}")

# Build node dict and parent/child mappings
# status: 0 = aborted, 1 = committed
nodes = {n['id']: n['status'] for n in nodes_list}
parents = defaultdict(list)
children = defaultdict(list)

for edge in edges_list:
    parent, child = edge[0], edge[1]
    parents[child].append(parent)
    children[parent].append(child)

# Count aborted and committed
aborted = [nid for nid, status in nodes.items() if status == 0]
committed = [nid for nid, status in nodes.items() if status == 1]

print(f"\nAborted: {len(aborted)}")
print(f"Committed: {len(committed)}")

# Check invariant: if parent aborted, child MUST have aborted
print("\n" + "="*60)
print("CHECKING INVARIANT: Aborted parent -> child must abort")
print("="*60)

violations = []
for child_id, child_status in nodes.items():
    child_parents = parents.get(child_id, [])

    for parent_id in child_parents:
        if parent_id not in nodes:
            continue  # Parent not in our dataset (unknown)

        parent_status = nodes[parent_id]

        # If parent aborted (0) but child committed (1), that's a violation
        if parent_status == 0 and child_status == 1:
            violations.append({
                'child': child_id,
                'parent': parent_id,
                'child_status': child_status,
                'parent_status': parent_status
            })

if violations:
    print(f"\nFOUND {len(violations)} VIOLATIONS!")
    print("These children committed despite having an aborted parent:\n")
    for v in violations[:10]:
        print(f"  Child {v['child'][:16]}... (status={v['child_status']})")
        print(f"    <- Parent {v['parent'][:16]}... (status={v['parent_status']} ABORTED)")
        print()
    if len(violations) > 10:
        print(f"  ... and {len(violations) - 10} more violations")
else:
    print("\nNO VIOLATIONS FOUND - all children of aborted parents also aborted.")

# Now properly categorize
print("\n" + "="*60)
print("PROPER CATEGORIZATION")
print("="*60)

# Categories:
# - committed: status=1, all parents committed (or no parents)
# - artificially_aborted: status=0, NO aborted parent (root cause)
# - cascadingly_aborted: status=0, has at least one aborted parent
# - unknown: has a parent not in our node set

categories = {
    'committed': [],
    'artificially_aborted': [],
    'cascadingly_aborted': [],
    'unknown': []
}

for nid, status in nodes.items():
    node_parents = parents.get(nid, [])

    # Check for missing parents (unknown)
    missing_parents = [p for p in node_parents if p not in nodes]

    if missing_parents:
        categories['unknown'].append(nid)
    elif status == 1:  # Committed
        categories['committed'].append(nid)
    elif status == 0:  # Aborted
        # Check if any parent is aborted
        has_aborted_parent = any(nodes.get(p, 1) == 0 for p in node_parents)
        if has_aborted_parent:
            categories['cascadingly_aborted'].append(nid)
        else:
            categories['artificially_aborted'].append(nid)

print("\nCategories:")
for cat, ids in categories.items():
    pct = len(ids) / len(nodes) * 100 if nodes else 0
    print(f"  {cat}: {len(ids)} ({pct:.2f}%)")

# Verify: committed nodes should NOT have any aborted parents
print("\n" + "="*60)
print("VERIFYING COMMITTED NODES")
print("="*60)

committed_with_aborted_parent = []
for nid in categories['committed']:
    node_parents = parents.get(nid, [])
    aborted_parents = [p for p in node_parents if p in nodes and nodes[p] == 0]
    if aborted_parents:
        committed_with_aborted_parent.append((nid, aborted_parents))

if committed_with_aborted_parent:
    print(f"\nERROR: {len(committed_with_aborted_parent)} committed nodes have aborted parents!")
    for nid, ab_parents in committed_with_aborted_parent[:5]:
        print(f"  {nid[:16]}... has aborted parents: {[p[:16] for p in ab_parents]}")
else:
    print("\nOK: All committed nodes have only committed parents (or parents not in dataset)")

# Show cascade chains
print("\n" + "="*60)
print("CASCADE CHAINS FROM ROOT ABORTS")
print("="*60)

for i, root_id in enumerate(categories['artificially_aborted']):
    print(f"\n--- Chain {i+1}: Root {root_id[:16]}... ---")

    # BFS to find all descendants
    visited = set([root_id])
    queue = [(root_id, 0)]
    level_nodes = defaultdict(list)

    while queue:
        nid, level = queue.pop(0)
        level_nodes[level].append((nid, nodes[nid]))

        for child in children.get(nid, []):
            if child in nodes and child not in visited:
                visited.add(child)
                queue.append((child, level + 1))

    for level in sorted(level_nodes.keys()):
        nodes_at_level = level_nodes[level]
        aborted_at_level = [(n, s) for n, s in nodes_at_level if s == 0]
        committed_at_level = [(n, s) for n, s in nodes_at_level if s == 1]

        print(f"  Level {level}:")
        if level == 0:
            print(f"    ROOT ABORT: {root_id[:16]}...")
        else:
            if aborted_at_level:
                print(f"    Cascaded aborts: {len(aborted_at_level)}")
            if committed_at_level:
                print(f"    Committed: {len(committed_at_level)} <- THIS WOULD BE A BUG IF PARENT ABORTED")
