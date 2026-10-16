import 'dart:async';

import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:go_router/go_router.dart';
import '../api/stralis_api.dart';
import '../models/job.dart';
import '../ui/status.dart';

final apiProvider = Provider<StralisApi>((ref) {
  return StralisApi(baseUrl: const String.fromEnvironment('STRALIS_API_URL', defaultValue: 'http://127.0.0.1:8100'));
});

final jobsProvider = FutureProvider<List<Job>>((ref) async {
  return ref.watch(apiProvider).listJobs(limit: 50);
});

class DashboardPage extends ConsumerStatefulWidget {
  const DashboardPage({super.key});

  @override
  ConsumerState<DashboardPage> createState() => _DashboardPageState();
}

class _DashboardPageState extends ConsumerState<DashboardPage> {
  Timer? _poll;

  @override
  void initState() {
    super.initState();
    // Keep the dashboard live while anything is running.
    _poll = Timer.periodic(const Duration(seconds: 3), (_) {
      ref.invalidate(jobsProvider);
    });
  }

  @override
  void dispose() {
    _poll?.cancel();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
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
              final c = statusColor(j.status);
              return ListTile(
                title: Text(j.name),
                subtitle: Text(j.id.substring(0, 8)),
                trailing: Chip(
                  label: Text(j.status.toUpperCase()),
                  backgroundColor: c.withValues(alpha: .1),
                  labelStyle: TextStyle(color: c),
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
