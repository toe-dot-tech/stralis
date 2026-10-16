import 'package:flutter/material.dart';
import 'package:graphview/GraphView.dart';

import '../ui/status.dart';

/// Interactive DAG rendering of a pipeline spec.
///
/// [nodes] entries need at least `id` and `handler`; optional `status` (a
/// NodeStatus value) colors the node to mirror live execution state.
class DagView extends StatelessWidget {
  final List<Map<String, dynamic>> nodes;
  final List<Map<String, dynamic>> edges;
  const DagView({super.key, required this.nodes, required this.edges});

  @override
  Widget build(BuildContext context) {
    final graph = Graph();
    final nodeMap = <String, Node>{};
    for (final n in nodes) {
      final id = n['id'] as String;
      nodeMap[id] = Node.Id(id);
      graph.addNode(nodeMap[id]!);
    }
    for (final e in edges) {
      final a = nodeMap[e['from']];
      final b = nodeMap[e['to']];
      if (a != null && b != null) graph.addEdge(a, b);
    }

    final builder = BuchheimWalkerConfiguration()
      ..siblingSeparation = 40
      ..levelSeparation = 70
      ..subtreeSeparation = 60;

    final scheme = Theme.of(context).colorScheme;
    return InteractiveViewer(
      constrained: false,
      boundaryMargin: const EdgeInsets.all(40),
      minScale: 0.4,
      maxScale: 2.5,
      child: GraphView(
        graph: graph,
        algorithm: BuchheimWalkerAlgorithm(builder, TreeEdgeRenderer(builder)),
        paint: Paint()
          ..color = scheme.outlineVariant
          ..strokeWidth = 1.4,
        builder: (Node node) {
          final id = node.key?.value as String? ?? '';
          final n = nodes.firstWhere(
            (e) => e['id'] == id,
            orElse: () => {'id': id, 'handler': ''},
          );
          final status = n['status'] as String?;
          final color = status == null ? scheme.outline : statusColor(status);
          return Container(
            padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 8),
            decoration: BoxDecoration(
              color: color.withValues(alpha: .10),
              border: Border.all(color: color, width: 1.5),
              borderRadius: BorderRadius.circular(10),
            ),
            child: Column(
              mainAxisSize: MainAxisSize.min,
              children: [
                Text(id, style: const TextStyle(fontWeight: FontWeight.bold)),
                if ((n['handler'] as String?)?.isNotEmpty == true)
                  Text(
                    n['handler'] as String,
                    style: Theme.of(context).textTheme.labelSmall,
                  ),
                if (status != null)
                  Text(
                    status.toUpperCase(),
                    style: Theme.of(context)
                        .textTheme
                        .labelSmall
                        ?.copyWith(color: color, fontWeight: FontWeight.w600),
                  ),
              ],
            ),
          );
        },
      ),
    );
  }
}
