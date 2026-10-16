import 'dart:async';

import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';

import '../ui/status.dart';
import '../widgets/dag_view.dart';
import 'dashboard_page.dart';

final jobDetailProvider = FutureProvider.family((ref, String id) async {
  return ref.watch(apiProvider).getJob(id);
});

const _terminalStatuses = {'succeeded', 'failed', 'cancelled'};

class JobDetailPage extends ConsumerStatefulWidget {
  final String jobId;
  const JobDetailPage({super.key, required this.jobId});

  @override
  ConsumerState<JobDetailPage> createState() => _JobDetailPageState();
}

class _JobDetailPageState extends ConsumerState<JobDetailPage> {
  Timer? _poll;

  @override
  void initState() {
    super.initState();
    _poll = Timer.periodic(const Duration(seconds: 2), (_) {
      ref.invalidate(jobDetailProvider(widget.jobId));
    });
  }

  @override
  void dispose() {
    _poll?.cancel();
    super.dispose();
  }

  void _stopPollingIfDone(AsyncValue detail) {
    final status = detail.valueOrNull?.job.status.toLowerCase() ?? '';
    if (_terminalStatuses.contains(status)) {
      _poll?.cancel();
      _poll = null;
    }
  }

  Future<void> _retry() async {
    try {
      await ref.read(apiProvider).retryJob(widget.jobId);
      _poll ??= Timer.periodic(const Duration(seconds: 2), (_) {
        ref.invalidate(jobDetailProvider(widget.jobId));
      });
      ref.invalidate(jobDetailProvider(widget.jobId));
    } catch (e) {
      if (mounted) {
        ScaffoldMessenger.of(context)
            .showSnackBar(SnackBar(content: Text('Retry failed: $e')));
      }
    }
  }

  @override
  Widget build(BuildContext context) {
    final detailAsync = ref.watch(jobDetailProvider(widget.jobId));
    ref.listen(jobDetailProvider(widget.jobId), (prev, next) {
      _stopPollingIfDone(next);
    });
    if (_poll == null && !detailAsync.hasValue) {
      // Terminal state was cached, then the job was retried elsewhere — resume.
      final status = detailAsync.valueOrNull?.job.status.toLowerCase() ?? '';
      if (!_terminalStatuses.contains(status)) {
        _poll = Timer.periodic(const Duration(seconds: 2), (_) {
          ref.invalidate(jobDetailProvider(widget.jobId));
        });
      }
    }

    return Scaffold(
      appBar: AppBar(
        title: Text('Job ${widget.jobId.substring(0, 8)}'),
        actions: [
          IconButton(
            icon: const Icon(Icons.refresh),
            tooltip: 'Refresh',
            onPressed: () => ref.invalidate(jobDetailProvider(widget.jobId)),
          ),
        ],
      ),
      body: detailAsync.when(
        loading: () => const Center(child: CircularProgressIndicator()),
        error: (e, _) => Center(child: Text('Error: $e')),
        data: (d) {
          final job = d.job;
          final status = job.status.toLowerCase();
          final statusC = statusColor(job.status);

          // Spec-driven graph: one edge per declared depends_on entry.
          final specNodes = (job.spec?['nodes'] as List? ?? [])
              .cast<Map<String, dynamic>>();
          final runStatus = {
            for (final r in d.nodeRuns) r.nodeId: r.status,
          };
          final graphNodes = [
            for (final n in specNodes)
              {
                'id': n['id'],
                'handler': n['handler'] ?? '',
                'status': runStatus[n['id']],
              },
          ];
          final edges = [
            for (final n in specNodes)
              for (final dep in (n['depends_on'] as List? ?? []))
                {'from': dep, 'to': n['id']},
          ];

          final runsList = ListView.separated(
            itemCount: d.nodeRuns.length,
            separatorBuilder: (_, __) => const Divider(height: 1),
            itemBuilder: (context, i) {
              final n = d.nodeRuns[i];
              final c = statusColor(n.status);
              return ListTile(
                dense: true,
                title: Text(n.nodeId,
                    style: const TextStyle(fontWeight: FontWeight.w600)),
                subtitle: Text([
                  n.handler,
                  'attempt ${n.attempt}/${n.maxAttempts}',
                  if (n.error != null && n.error!.isNotEmpty)
                    'error: ${n.error}',
                ].join(' • ')),
                trailing: Chip(
                  label: Text(n.status.toUpperCase()),
                  backgroundColor: c.withValues(alpha: .12),
                  labelStyle:
                      TextStyle(color: c, fontWeight: FontWeight.w600),
                  visualDensity: VisualDensity.compact,
                ),
              );
            },
          );

          return Padding(
            padding: const EdgeInsets.all(16),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Row(
                  children: [
                    Chip(
                      label: Text(job.status.toUpperCase()),
                      backgroundColor: statusC.withValues(alpha: .12),
                      labelStyle: TextStyle(
                          color: statusC, fontWeight: FontWeight.bold),
                    ),
                    const SizedBox(width: 12),
                    Expanded(
                      child: Text(job.name,
                          style: Theme.of(context).textTheme.titleMedium,
                          overflow: TextOverflow.ellipsis),
                    ),
                    if (status == 'failed')
                      FilledButton.icon(
                        onPressed: _retry,
                        icon: const Icon(Icons.replay, size: 18),
                        label: const Text('Retry'),
                      ),
                  ],
                ),
                if (job.error != null && job.error!.isNotEmpty) ...[
                  const SizedBox(height: 8),
                  Text(job.error!,
                      style:
                          TextStyle(color: Theme.of(context).colorScheme.error)),
                ],
                const SizedBox(height: 16),
                Expanded(
                  child: LayoutBuilder(
                    builder: (context, box) {
                      final wide = box.maxWidth > 800;
                      final dag = Card(
                        margin: EdgeInsets.zero,
                        child: DagView(nodes: graphNodes, edges: edges),
                      );
                      final runs = Column(
                        crossAxisAlignment: CrossAxisAlignment.start,
                        children: [
                          Text('Node runs',
                              style: Theme.of(context).textTheme.titleSmall),
                          const SizedBox(height: 8),
                          Expanded(child: runsList),
                        ],
                      );
                      if (wide) {
                        return Row(
                          crossAxisAlignment: CrossAxisAlignment.start,
                          children: [
                            Expanded(flex: 3, child: dag),
                            const SizedBox(width: 16),
                            Expanded(flex: 2, child: runs),
                          ],
                        );
                      }
                      return Column(
                        children: [
                          SizedBox(height: 260, child: dag),
                          const SizedBox(height: 16),
                          Expanded(child: runs),
                        ],
                      );
                    },
                  ),
                ),
              ],
            ),
          );
        },
      ),
    );
  }
}
