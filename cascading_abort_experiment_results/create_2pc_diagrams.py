#!/usr/bin/env python3
"""
Create visual diagrams showing Pipelined 2PC with cascading aborts.
Two scenarios: Happy path and Cascading abort path.
"""

import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import matplotlib.lines as mlines

# Color scheme
COORD_COLOR = '#E8F4F8'      # Light blue
RANGE_COLOR = '#F0F8E8'      # Light green
RESOLVER_COLOR = '#FFF4E6'   # Light orange
ARROW_COLOR = '#2E86AB'
ABORT_COLOR = '#FFE5E5'      # Light red
SUCCESS_COLOR = '#E8F8E8'    # Light green

def create_component_box(ax, x, y, width, height, title, content, bg_color, is_abort=False):
    """Create a component box showing data structures"""
    # Background box
    if is_abort:
        bg_color = ABORT_COLOR

    box = FancyBboxPatch((x, y), width, height,
                         boxstyle="round,pad=0.05",
                         edgecolor='black', facecolor=bg_color,
                         linewidth=2 if is_abort else 1)
    ax.add_patch(box)

    # Title
    ax.text(x + width/2, y + height - 0.15, title,
            fontsize=11, fontweight='bold', ha='center',
            bbox=dict(boxstyle='round,pad=0.3', facecolor='white', edgecolor='black'))

    # Content
    y_offset = y + height - 0.5
    for line in content:
        ax.text(x + 0.1, y_offset, line, fontsize=8, family='monospace',
                verticalalignment='top')
        y_offset -= 0.25

def draw_arrow(ax, x1, y1, x2, y2, label='', color=ARROW_COLOR, style='->'):
    """Draw an arrow between components"""
    arrow = FancyArrowPatch((x1, y1), (x2, y2),
                           arrowstyle=style, color=color, linewidth=2,
                           mutation_scale=20, zorder=1)
    ax.add_patch(arrow)

    if label:
        mid_x, mid_y = (x1 + x2) / 2, (y1 + y2) / 2
        ax.text(mid_x, mid_y + 0.1, label, fontsize=9,
                bbox=dict(boxstyle='round,pad=0.3', facecolor='white', edgecolor=color),
                ha='center', color=color, fontweight='bold')

def create_phase_diagram(scenario_name, phase_num, phase_title, components_data, arrows_data, output_file):
    """Create a single phase diagram"""
    fig, ax = plt.subplots(figsize=(14, 10))
    ax.set_xlim(0, 14)
    ax.set_ylim(0, 10)
    ax.axis('off')

    # Title
    fig.suptitle(f'{scenario_name}\nPhase {phase_num}: {phase_title}',
                 fontsize=16, fontweight='bold', y=0.98)

    # Draw components
    for comp in components_data:
        create_component_box(ax, comp['x'], comp['y'], comp['width'], comp['height'],
                           comp['title'], comp['content'], comp['color'],
                           comp.get('is_abort', False))

    # Draw arrows
    for arrow in arrows_data:
        draw_arrow(ax, arrow['x1'], arrow['y1'], arrow['x2'], arrow['y2'],
                  arrow.get('label', ''), arrow.get('color', ARROW_COLOR),
                  arrow.get('style', '->'))

    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"✓ Created {output_file}")


# ============================================================================
# HAPPY PATH DIAGRAMS
# ============================================================================

def create_happy_path_diagrams():
    """Create all happy path scenario diagrams"""

    # PHASE 1: T1 PREPARE
    components = [
        {
            'x': 0.5, 'y': 6.5, 'width': 4, 'height': 3,
            'title': 'Coordinator (T1)',
            'color': COORD_COLOR,
            'content': [
                'Transaction {',
                '  id: T1',
                '  dependencies: []',
                '  state: Running',
                '}',
                '',
                'DependencyTracker {',
                '  reverse_deps: {}',
                '  aborting_txns: {}',
                '}'
            ]
        },
        {
            'x': 5.5, 'y': 6.5, 'width': 4, 'height': 3,
            'title': 'Range0 (after T1 PREPARE)',
            'color': RANGE_COLOR,
            'content': [
                'pending_prepare_records: {',
                '  T1: {changes: {A: 1}}',
                '}',
                '',
                'pending_commit_table: {',
                '  A: T1  ← last writer',
                '}',
                '',
                'key_version_chain: {',
                '  A: [T1]  ← history',
                '}'
            ]
        }
    ]

    arrows = [
        {'x1': 2.5, 'y1': 7.0, 'x2': 5.3, 'y2': 7.0, 'label': 'PREPARE(T1)', 'style': '->'},
        {'x1': 5.3, 'y1': 6.5, 'x2': 2.5, 'y2': 6.5, 'label': 'PrepareResult\n{deps:[], early_release:true}', 'style': '<-'}
    ]

    create_phase_diagram('HAPPY PATH', 1, 'T1 PREPARE & Lock Release',
                        components, arrows, 'happy_path_phase1_t1_prepare.png')

    # PHASE 2: T1 COMMIT
    components = [
        {
            'x': 0.5, 'y': 6.5, 'width': 4, 'height': 3,
            'title': 'Coordinator (T1)',
            'color': SUCCESS_COLOR,
            'content': [
                'Transaction {',
                '  id: T1',
                '  dependencies: []  ← none!',
                '  state: Committed ✓',
                '}',
                '',
                'Direct commit (no resolver)',
                'tx_state_store.commit(T1)'
            ]
        },
        {
            'x': 5.5, 'y': 6.5, 'width': 4, 'height': 3,
            'title': 'Range0 (after T1 COMMIT)',
            'color': RANGE_COLOR,
            'content': [
                'pending_prepare_records: {',
                '  // T1 removed',
                '}',
                '',
                'pending_commit_table: {',
                '  A: T1  ← still here!',
                '}',
                '',
                'key_version_chain: {',
                '  A: [T1]  ← for tracking',
                '}'
            ]
        }
    ]

    arrows = [
        {'x1': 2.5, 'y1': 7.0, 'x2': 5.3, 'y2': 7.0, 'label': 'COMMIT(T1)', 'color': 'green'}
    ]

    create_phase_diagram('HAPPY PATH', 2, 'T1 COMMIT (Direct - No Dependencies)',
                        components, arrows, 'happy_path_phase2_t1_commit.png')

    # PHASE 3: T2 GET(A) - Creates Dependency
    components = [
        {
            'x': 0.5, 'y': 6.5, 'width': 4, 'height': 3,
            'title': 'Coordinator (T2)',
            'color': COORD_COLOR,
            'content': [
                'Transaction {',
                '  id: T2',
                '  dependencies: [T1]  ← NEW!',
                '  state: Running',
                '}',
                '',
                'DependencyTracker {',
                '  reverse_deps: {',
                '    T1: [T2]  ← T2 depends on T1',
                '  }',
                '}'
            ]
        },
        {
            'x': 5.5, 'y': 6.5, 'width': 4, 'height': 3,
            'title': 'Range0 (T2 GET processing)',
            'color': RANGE_COLOR,
            'content': [
                'Check pending_commit_table[A]:',
                '  → T1 (found!)',
                '',
                'Check pending_prepare_records[T1]:',
                '  → None (T1 committed)',
                '',
                'Action: Read from Cassandra',
                'Return: GetResult {',
                '  val: 1  (T1\'s committed val)',
                '  dependencies: []  ← clean!',
                '}'
            ]
        }
    ]

    arrows = [
        {'x1': 2.5, 'y1': 7.5, 'x2': 5.3, 'y2': 7.5, 'label': 'GET(A)'},
        {'x1': 5.3, 'y1': 7.0, 'x2': 2.5, 'y2': 7.0, 'label': 'val=1, deps=[]', 'style': '<-'}
    ]

    create_phase_diagram('HAPPY PATH', 3, 'T2 GET(A) - No Dependency (T1 committed)',
                        components, arrows, 'happy_path_phase3_t2_get.png')

    # PHASE 4: T3 GET(B) - Creates Dependency on Uncommitted T2
    components = [
        {
            'x': 0.5, 'y': 6.5, 'width': 4, 'height': 3,
            'title': 'Coordinator (T3)',
            'color': COORD_COLOR,
            'content': [
                'Transaction {',
                '  id: T3',
                '  dependencies: [T2]  ← NEW!',
                '  state: Running',
                '}',
                '',
                'DependencyTracker {',
                '  reverse_deps: {',
                '    T1: [T2],',
                '    T2: [T3]  ← T3 depends on T2',
                '  }',
                '}'
            ]
        },
        {
            'x': 5.5, 'y': 6.5, 'width': 4, 'height': 3,
            'title': 'Range0 (T3 GET processing)',
            'color': RANGE_COLOR,
            'content': [
                'Check pending_commit_table[B]:',
                '  → T2 (found!)',
                '',
                'Check pending_prepare_records[T2]:',
                '  → PrepareRecord {changes:{B:2}}',
                '  T2 still UNCOMMITTED!',
                '',
                'Return: GetResult {',
                '  val: 2  (T2\'s uncommitted)',
                '  dependencies: [T2]  ← DIRTY!',
                '}'
            ]
        }
    ]

    arrows = [
        {'x1': 2.5, 'y1': 7.5, 'x2': 5.3, 'y2': 7.5, 'label': 'GET(B)'},
        {'x1': 5.3, 'y1': 7.0, 'x2': 2.5, 'y2': 7.0, 'label': 'val=2, deps=[T2]', 'style': '<-', 'color': 'orange'}
    ]

    create_phase_diagram('HAPPY PATH', 4, 'T3 GET(B) - Dependency on Uncommitted T2',
                        components, arrows, 'happy_path_phase4_t3_get.png')

    # PHASE 5: T3 COMMIT via Resolver
    components = [
        {
            'x': 0.5, 'y': 6.0, 'width': 3.5, 'height': 3.5,
            'title': 'Coordinator (T3)',
            'color': COORD_COLOR,
            'content': [
                'Transaction {',
                '  id: T3',
                '  dependencies: [T2]',
                '  state: Running',
                '}',
                '',
                '// Validation check:',
                'is_aborting(T2)? → false ✓',
                '',
                '// Send to resolver',
                'resolver.commit(T3, deps=[T2])'
            ]
        },
        {
            'x': 4.5, 'y': 6.0, 'width': 3.5, 'height': 3.5,
            'title': 'Resolver',
            'color': RESOLVER_COLOR,
            'content': [
                'Received: commit(T3, deps=[T2])',
                '',
                '1. Wait for dependencies:',
                '   wait_for(T2) → committed ✓',
                '',
                '2. All deps committed!',
                '   tx_state_store.commit(T3)',
                '',
                '3. Notify coordinator → success'
            ]
        },
        {
            'x': 8.5, 'y': 6.0, 'width': 3.5, 'height': 3.5,
            'title': 'Range0 (T3 COMMIT)',
            'color': SUCCESS_COLOR,
            'content': [
                'pending_prepare_records: {',
                '  // T3 removed after commit',
                '}',
                '',
                'All transactions committed:',
                'T1 ✓ → T2 ✓ → T3 ✓',
                '',
                'Dependency chain resolved!'
            ]
        }
    ]

    arrows = [
        {'x1': 2.2, 'y1': 7.5, 'x2': 4.3, 'y2': 7.5, 'label': 'commit(T3, [T2])', 'color': 'green'},
        {'x1': 6.2, 'y1': 7.0, 'x2': 8.3, 'y2': 7.0, 'label': 'COMMIT(T3)', 'color': 'green'}
    ]

    create_phase_diagram('HAPPY PATH', 5, 'T3 COMMIT via Resolver (waits for T2)',
                        components, arrows, 'happy_path_phase5_t3_resolver.png')

# ============================================================================
# CASCADING ABORT DIAGRAMS
# ============================================================================

def create_abort_path_diagrams():
    """Create all cascading abort scenario diagrams"""

    # PHASE 1: T1 PREPARE with Artificial Abort
    components = [
        {
            'x': 0.5, 'y': 6.0, 'width': 4, 'height': 3.5,
            'title': 'Coordinator (T1)',
            'color': ABORT_COLOR,
            'is_abort': True,
            'content': [
                'PREPARE failed!',
                'Error: TransactionAborted(Other)',
                '',
                'Calls record_abort():',
                '  1. Mark as aborting',
                '  2. Abort in tx_state_store',
                '  3. Send abort to ranges',
                '',
                'state: Aborted ✗'
            ]
        },
        {
            'x': 5.0, 'y': 6.0, 'width': 4, 'height': 3.5,
            'title': 'Range0 (T1 Artificial Abort)',
            'color': ABORT_COLOR,
            'is_abort': True,
            'content': [
                '🎲 Artificial abort triggered!',
                '',
                'Synchronous cleanup:',
                '  ✓ Remove from pending_prepare',
                '  ✓ Remove from version_chain',
                '  ✓ Remove from commit_table',
                '  ✓ Release lock',
                '',
                'Return: Err(TransactionAborted)'
            ]
        },
        {
            'x': 9.5, 'y': 6.0, 'width': 4, 'height': 3.5,
            'title': 'DependencyTracker (Global)',
            'color': COORD_COLOR,
            'content': [
                'mark_aborting([T1]):',
                '',
                'aborting_transactions: {',
                '  T1  ← PERMANENT!',
                '}',
                '',
                'reverse_deps: {}',
                '',
                '🔥 T1 marked forever'
            ]
        }
    ]

    arrows = [
        {'x1': 2.5, 'y1': 7.5, 'x2': 4.8, 'y2': 7.5, 'label': 'PREPARE(T1)', 'color': 'red'},
        {'x1': 4.8, 'y1': 7.0, 'x2': 2.5, 'y2': 7.0, 'label': 'Err(Aborted)', 'style': '<-', 'color': 'red'},
        {'x1': 2.5, 'y1': 6.5, 'x2': 9.3, 'y2': 7.5, 'label': 'mark_aborting(T1)', 'color': 'red', 'style': '->'}
    ]

    create_phase_diagram('CASCADING ABORT', 1, 'T1 Artificial Abort & Cleanup',
                        components, arrows, 'abort_path_phase1_t1_abort.png')

    # PHASE 2: T2 GET(A) - Race Condition
    components = [
        {
            'x': 0.5, 'y': 6.0, 'width': 4, 'height': 3.5,
            'title': 'Coordinator (T2)',
            'color': COORD_COLOR,
            'content': [
                'Transaction {',
                '  id: T2',
                '  dependencies: [T1]  ← RACE!',
                '  state: Running',
                '}',
                '',
                'T2 read A before T1 cleanup!',
                'Got uncommitted value from T1'
            ]
        },
        {
            'x': 5.0, 'y': 6.0, 'width': 4, 'height': 3.5,
            'title': 'Range0 (T2 GET race)',
            'color': RANGE_COLOR,
            'content': [
                'pending_commit_table[A]: T1',
                'pending_prepare_records[T1]:',
                '  PrepareRecord {A: 1}  ← still here!',
                '',
                'Return: GetResult {',
                '  val: 1  (T1 uncommitted)',
                '  dependencies: [T1]  ← dirty!',
                '}',
                '',
                '⚠️  T2 depends on aborting T1!'
            ]
        },
        {
            'x': 9.5, 'y': 6.0, 'width': 4, 'height': 3.5,
            'title': 'DependencyTracker',
            'color': COORD_COLOR,
            'content': [
                'aborting_transactions: {',
                '  T1  ← T1 is aborting!',
                '}',
                '',
                'reverse_deps: {',
                '  T1: [T2]  ← T2 depends on T1',
                '}',
                '',
                '⚠️  Dangerous dependency!'
            ]
        }
    ]

    arrows = [
        {'x1': 2.5, 'y1': 7.5, 'x2': 4.8, 'y2': 7.5, 'label': 'GET(A)', 'color': 'orange'},
        {'x1': 4.8, 'y1': 7.0, 'x2': 2.5, 'y2': 7.0, 'label': 'val=1, deps=[T1]', 'style': '<-', 'color': 'orange'},
        {'x1': 2.5, 'y1': 6.5, 'x2': 9.3, 'y2': 7.5, 'label': 'add_reverse_dep(T1,T2)', 'color': 'orange'}
    ]

    create_phase_diagram('CASCADING ABORT', 2, 'T2 GET(A) - Race: Depends on Aborting T1',
                        components, arrows, 'abort_path_phase2_t2_race.png')

    # PHASE 3: T2 COMMIT - Dependency Validation Catches Abort
    components = [
        {
            'x': 0.5, 'y': 5.5, 'width': 4.5, 'height': 4,
            'title': 'Coordinator (T2 Validation)',
            'color': ABORT_COLOR,
            'is_abort': True,
            'content': [
                'Transaction {',
                '  id: T2',
                '  dependencies: [T1]',
                '}',
                '',
                '🔍 Dependency validation:',
                'for dep in dependencies:',
                '  is_aborting(T1)?',
                '    → TRUE! ✗',
                '',
                '🛑 Prevent resolver deadlock!',
                'Call cascade_abort(T2)'
            ]
        },
        {
            'x': 5.5, 'y': 5.5, 'width': 4, 'height': 4,
            'title': 'Resolver (NOT REACHED)',
            'color': RESOLVER_COLOR,
            'content': [
                '❌ T2 never sent here!',
                '',
                'If T2 was sent:',
                '  wait_for(T1)',
                '  → DEADLOCK! (T1 never commits)',
                '',
                '✓ Validation prevented deadlock',
                '',
                'This is the key insight!'
            ]
        },
        {
            'x': 10.0, 'y': 5.5, 'width': 3.5, 'height': 4,
            'title': 'DependencyTracker',
            'color': ABORT_COLOR,
            'is_abort': True,
            'content': [
                'aborting_transactions: {',
                '  T1,  ← was aborting',
                '  T2   ← now aborting',
                '}',
                '',
                'reverse_deps: {',
                '  T2: [T3]  ← T3 must abort',
                '}'
            ]
        }
    ]

    arrows = [
        {'x1': 2.7, 'y1': 7.0, 'x2': 5.3, 'y2': 7.0, 'label': '❌ BLOCKED', 'color': 'red', 'style': '-'},
        {'x1': 2.7, 'y1': 6.0, 'x2': 9.8, 'y2': 7.0, 'label': 'mark_aborting(T2)', 'color': 'red'}
    ]

    create_phase_diagram('CASCADING ABORT', 3, 'T2 COMMIT - Validation Prevents Resolver Deadlock',
                        components, arrows, 'abort_path_phase3_t2_validation.png')

    # PHASE 4: Cascade Abort Propagation
    components = [
        {
            'x': 0.5, 'y': 5.0, 'width': 4.5, 'height': 4.5,
            'title': 'Coordinator: cascade_abort(T2)',
            'color': ABORT_COLOR,
            'is_abort': True,
            'content': [
                '1. Find all dependents:',
                '   reverse_deps[T2] = [T3]',
                '   to_abort = {T2, T3}',
                '',
                '2. Mark all as aborting:',
                '   mark_aborting([T2, T3])',
                '',
                '3. Abort in leaf order:',
                '   - Abort T3 (no dependents)',
                '   - Abort T2',
                '',
                'Result: T2, T3 aborted ✗'
            ]
        },
        {
            'x': 5.5, 'y': 5.0, 'width': 4, 'height': 4.5,
            'title': 'Range0 (Abort Messages)',
            'color': ABORT_COLOR,
            'is_abort': True,
            'content': [
                'Receives abort for T3:',
                '  ✓ Remove pending_prepare[T3]',
                '  ✓ Clean version_chain',
                '  ✓ Clean commit_table',
                '',
                'Receives abort for T2:',
                '  ✓ Remove pending_prepare[T2]',
                '  ✓ Clean version_chain',
                '  ✓ Clean commit_table',
                '',
                'All aborted transactions cleaned'
            ]
        },
        {
            'x': 10.0, 'y': 5.0, 'width': 3.5, 'height': 4.5,
            'title': 'DependencyTracker (Final)',
            'color': ABORT_COLOR,
            'is_abort': True,
            'content': [
                'aborting_transactions: {',
                '  T1,  ← permanent',
                '  T2,  ← permanent',
                '  T3   ← permanent',
                '}',
                '',
                'reverse_deps: {}',
                '  // cleaned up',
                '',
                '🔥 All marked forever'
            ]
        }
    ]

    arrows = [
        {'x1': 2.7, 'y1': 7.0, 'x2': 5.3, 'y2': 7.0, 'label': 'ABORT(T3)', 'color': 'red'},
        {'x1': 2.7, 'y1': 6.5, 'x2': 5.3, 'y2': 6.5, 'label': 'ABORT(T2)', 'color': 'red'},
        {'x1': 2.7, 'y1': 5.5, 'x2': 9.8, 'y2': 6.5, 'label': 'mark_aborting([T2,T3])', 'color': 'red'}
    ]

    create_phase_diagram('CASCADING ABORT', 4, 'Cascade Abort: T2→T3 Propagation',
                        components, arrows, 'abort_path_phase4_cascade.png')

# ============================================================================
# SUMMARY COMPARISON DIAGRAM
# ============================================================================

def create_summary_diagram():
    """Create a summary comparison diagram"""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 10))

    # Happy Path Summary
    ax1.set_xlim(0, 7)
    ax1.set_ylim(0, 10)
    ax1.axis('off')
    ax1.set_title('HAPPY PATH: All Transactions Commit', fontsize=14, fontweight='bold', pad=20)

    # Timeline
    timeline_y = 8
    ax1.text(3.5, timeline_y + 0.5, 'Timeline', fontsize=12, fontweight='bold', ha='center')

    # T1
    box1 = FancyBboxPatch((0.5, timeline_y - 1), 2, 0.6, boxstyle="round,pad=0.05",
                          edgecolor='black', facecolor=SUCCESS_COLOR, linewidth=2)
    ax1.add_patch(box1)
    ax1.text(1.5, timeline_y - 0.7, 'T1: PREPARE→COMMIT ✓', ha='center', fontsize=10, fontweight='bold')
    ax1.text(1.5, timeline_y - 1.3, 'deps: []', ha='center', fontsize=8)
    ax1.text(1.5, timeline_y - 1.5, 'Direct commit', ha='center', fontsize=8, style='italic')

    # T2
    box2 = FancyBboxPatch((2.7, timeline_y - 2.5), 2, 0.6, boxstyle="round,pad=0.05",
                          edgecolor='black', facecolor=SUCCESS_COLOR, linewidth=2)
    ax1.add_patch(box2)
    ax1.text(3.7, timeline_y - 2.2, 'T2: GET(A)→PREPARE→COMMIT ✓', ha='center', fontsize=10, fontweight='bold')
    ax1.text(3.7, timeline_y - 2.8, 'deps: [] (T1 committed)', ha='center', fontsize=8)
    ax1.text(3.7, timeline_y - 3.0, 'Direct commit', ha='center', fontsize=8, style='italic')

    # T3
    box3 = FancyBboxPatch((4.9, timeline_y - 4), 2, 0.6, boxstyle="round,pad=0.05",
                          edgecolor='black', facecolor=SUCCESS_COLOR, linewidth=2)
    ax1.add_patch(box3)
    ax1.text(5.9, timeline_y - 3.7, 'T3: GET(B)→PREPARE→COMMIT ✓', ha='center', fontsize=10, fontweight='bold')
    ax1.text(5.9, timeline_y - 4.3, 'deps: [T2] (uncommitted)', ha='center', fontsize=8)
    ax1.text(5.9, timeline_y - 4.5, 'Via resolver', ha='center', fontsize=8, style='italic')

    # Dependencies
    ax1.arrow(2.5, timeline_y - 0.9, 1.0, -1.3, head_width=0.15, head_length=0.1, fc='green', ec='green')
    ax1.text(2.8, timeline_y - 1.8, 'T1 committed\nbefore T2 read', fontsize=7, ha='center',
             bbox=dict(boxstyle='round,pad=0.3', facecolor='white', edgecolor='green'))

    ax1.arrow(4.7, timeline_y - 2.4, 1.0, -1.3, head_width=0.15, head_length=0.1, fc='orange', ec='orange')
    ax1.text(5.0, timeline_y - 3.3, 'T2 uncommitted\nwhen T3 read', fontsize=7, ha='center',
             bbox=dict(boxstyle='round,pad=0.3', facecolor='white', edgecolor='orange'))

    # Key insight
    ax1.text(3.5, 1.5, '✓ Key Insight:', fontsize=11, fontweight='bold', ha='center', color='green')
    ax1.text(3.5, 0.8, 'T3 has dependency on T2\n→ Validation: is_aborting(T2)? FALSE\n→ Sent to resolver safely\n→ Resolver waits for T2 (which commits)\n→ T3 commits successfully',
             fontsize=9, ha='center',
             bbox=dict(boxstyle='round,pad=0.5', facecolor=SUCCESS_COLOR, edgecolor='green', linewidth=2))

    # Cascading Abort Summary
    ax2.set_xlim(0, 7)
    ax2.set_ylim(0, 10)
    ax2.axis('off')
    ax2.set_title('CASCADING ABORT: Abort Propagation', fontsize=14, fontweight='bold', pad=20)

    # Timeline
    ax2.text(3.5, timeline_y + 0.5, 'Timeline', fontsize=12, fontweight='bold', ha='center')

    # T1 abort
    box1 = FancyBboxPatch((0.5, timeline_y - 1), 2, 0.6, boxstyle="round,pad=0.05",
                          edgecolor='red', facecolor=ABORT_COLOR, linewidth=3)
    ax2.add_patch(box1)
    ax2.text(1.5, timeline_y - 0.7, 'T1: PREPARE→ABORT ✗', ha='center', fontsize=10, fontweight='bold')
    ax2.text(1.5, timeline_y - 1.3, '🎲 Artificial abort', ha='center', fontsize=8, color='red')
    ax2.text(1.5, timeline_y - 1.5, 'mark_aborting(T1)', ha='center', fontsize=8, style='italic')

    # T2 cascade abort
    box2 = FancyBboxPatch((2.7, timeline_y - 2.5), 2, 0.6, boxstyle="round,pad=0.05",
                          edgecolor='red', facecolor=ABORT_COLOR, linewidth=3)
    ax2.add_patch(box2)
    ax2.text(3.7, timeline_y - 2.2, 'T2: GET(A)→PREPARE→ABORT ✗', ha='center', fontsize=10, fontweight='bold')
    ax2.text(3.7, timeline_y - 2.8, 'deps: [T1] (aborting!)', ha='center', fontsize=8, color='red')
    ax2.text(3.7, timeline_y - 3.0, 'Validation caught it', ha='center', fontsize=8, style='italic')

    # T3 cascade abort
    box3 = FancyBboxPatch((4.9, timeline_y - 4), 2, 0.6, boxstyle="round,pad=0.05",
                          edgecolor='red', facecolor=ABORT_COLOR, linewidth=3)
    ax2.add_patch(box3)
    ax2.text(5.9, timeline_y - 3.7, 'T3: CASCADE ABORT ✗', ha='center', fontsize=10, fontweight='bold')
    ax2.text(5.9, timeline_y - 4.3, 'Transitive dependency', ha='center', fontsize=8, color='red')
    ax2.text(5.9, timeline_y - 4.5, 'Aborted in leaf order', ha='center', fontsize=8, style='italic')

    # Dependencies
    ax2.arrow(2.5, timeline_y - 0.9, 1.0, -1.3, head_width=0.15, head_length=0.1, fc='red', ec='red', linestyle='--')
    ax2.text(2.8, timeline_y - 1.8, 'T2 read from\naborting T1', fontsize=7, ha='center',
             bbox=dict(boxstyle='round,pad=0.3', facecolor=ABORT_COLOR, edgecolor='red'))

    ax2.arrow(4.7, timeline_y - 2.4, 1.0, -1.3, head_width=0.15, head_length=0.1, fc='red', ec='red', linestyle='--')
    ax2.text(5.0, timeline_y - 3.3, 'T3 depends on\naborting T2', fontsize=7, ha='center',
             bbox=dict(boxstyle='round,pad=0.3', facecolor=ABORT_COLOR, edgecolor='red'))

    # Key insight
    ax2.text(3.5, 1.5, '🛑 Key Insight:', fontsize=11, fontweight='bold', ha='center', color='red')
    ax2.text(3.5, 0.8, 'T2 has dependency on T1\n→ Validation: is_aborting(T1)? TRUE\n→ BLOCKED from resolver\n→ cascade_abort(T2) triggered\n→ T3 also aborted (transitive)',
             fontsize=9, ha='center',
             bbox=dict(boxstyle='round,pad=0.5', facecolor=ABORT_COLOR, edgecolor='red', linewidth=2))

    plt.tight_layout()
    plt.savefig('summary_comparison.png', dpi=300, bbox_inches='tight')
    plt.close()
    print("✓ Created summary_comparison.png")

# ============================================================================
# MAIN
# ============================================================================

if __name__ == '__main__':
    print("\n" + "="*60)
    print("Creating Pipelined 2PC Diagrams with Cascading Aborts")
    print("="*60 + "\n")

    print("Creating Happy Path diagrams...")
    create_happy_path_diagrams()

    print("\nCreating Cascading Abort diagrams...")
    create_abort_path_diagrams()

    print("\nCreating summary comparison...")
    create_summary_diagram()

    print("\n" + "="*60)
    print("✓ All diagrams created successfully!")
    print("="*60)
    print("\nHappy Path Diagrams:")
    print("  - happy_path_phase1_t1_prepare.png")
    print("  - happy_path_phase2_t1_commit.png")
    print("  - happy_path_phase3_t2_get.png")
    print("  - happy_path_phase4_t3_get.png")
    print("  - happy_path_phase5_t3_resolver.png")
    print("\nCascading Abort Diagrams:")
    print("  - abort_path_phase1_t1_abort.png")
    print("  - abort_path_phase2_t2_race.png")
    print("  - abort_path_phase3_t2_validation.png")
    print("  - abort_path_phase4_cascade.png")
    print("\nSummary:")
    print("  - summary_comparison.png")
    print("="*60 + "\n")
