import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:go_router/go_router.dart';
import '../api/stralis_api.dart';
import '../models/job.dart';

final apiProvider = Provider<StralisApi>((ref) {
  return StralisApi(baseUrl: const String.fromEnvironment('STRALIS_API_URL', defaultValue: 'http://127.0.0.1:8100'));
});

final jobsProvider = FutureProvider<List<Job>>((ref) async {
  return ref.watch(apiProvider).listJobs(limit: 50);
});

class DashboardPage extends ConsumerWidget {
  const DashboardPage({super.key});

  Color _statusColor(String s) {
    switch (s.toLowerCase()) {
      case 'succeeded':
        return Colors.green;
      case 'running':
        return Colors.blue;
      case 'pending':
        return Colors.orange;
      case 'failed':
        return Colors.red;
      case 'skipped':
        return Colors.grey;
      default:
        return Colors.black54;
    }
  }

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    final jobsAsync = ref.watch(jobsProvider);
    return Scaffold(
      appBar: AppBar(
        title: const Text('Stralis · Jobs'),
        actions: [
          IconButton(
            icon: const Icon(Icons.add),
            tooltip: 'New pipeline',
            onPressed: () => context.go('/editor'),
          ),
          IconButton(
            icon: const Icon(Icons.refresh),
            tooltip: 'Refresh',
            onPressed: () => ref.invalidate(jobsProvider),
          ),
        ],
      ),
      body: jobsAsync.when(
        loading: () => const Center(child: CircularProgressIndicator()),
        error: (e, _) => Center(child: Text('Error: $e')),
        data: (jobs) {
          if (jobs.isEmpty) return const Center(child: Text('No jobs yet'));
          return ListView.separated(
            itemCount: jobs.length,
            separatorBuilder: (_, __) => const Divider(height: 1),
            itemBuilder: (context, i) {
              final j = jobs[i];
              return ListTile(
                title: Text(j.name),
                subtitle: Text(j.id.substring(0, 8)),
                trailing: Chip(
                  label: Text(j.status.toUpperCase()),
                  backgroundColor: _statusColor(j.status).withOpacity(.1),
                  labelStyle: TextStyle(color: _statusColor(j.status)),
                ),
                onTap: () => context.go('/jobs/${j.id}'),
              );
            },
          );
        },
      ),
    );
  }
}
