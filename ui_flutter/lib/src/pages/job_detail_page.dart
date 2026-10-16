import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'dashboard_page.dart';

final jobDetailProvider = FutureProvider.family((ref, String id) async {
  return ref.watch(apiProvider).getJob(id);
});

class JobDetailPage extends ConsumerWidget {
  final String jobId;
  const JobDetailPage({super.key, required this.jobId});

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    final detailAsync = ref.watch(jobDetailProvider(jobId));
    return Scaffold(
      appBar: AppBar(
        title: Text('Job ${jobId.substring(0, 8)}'),
      ),
      body: detailAsync.when(
        loading: () => const Center(child: CircularProgressIndicator()),
        error: (e, _) => Center(child: Text('Error: $e')),
        data: (d) {
          return Padding(
            padding: const EdgeInsets.all(16),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text('Status: ${d.job.status}', style: Theme.of(context).textTheme.titleMedium),
                const SizedBox(height: 16),
                Text('Node runs', style: Theme.of(context).textTheme.titleSmall),
                const SizedBox(height: 8),
                Expanded(
                  child: ListView.separated(
                    itemCount: d.nodeRuns.length,
                    separatorBuilder: (_, __) => const Divider(),
                    itemBuilder: (context, i) {
                      final n = d.nodeRuns[i];
                      return ListTile(
                        title: Text(n.nodeId),
                        subtitle: Text('${n.handler} • attempt ${n.attempt}/${n.maxAttempts}'),
                        trailing: Text(n.status),
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
