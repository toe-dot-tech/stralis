import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:go_router/go_router.dart';
import 'src/pages/dashboard_page.dart';
import 'src/pages/job_detail_page.dart';
import 'src/pages/pipeline_editor_page.dart';

void main() {
  runApp(const ProviderScope(child: StralisApp()));
}

final _router = GoRouter(
  routes: [
    GoRoute(
      path: '/',
      builder: (context, state) => const DashboardPage(),
    ),
    GoRoute(
      path: '/editor',
      builder: (context, state) => const PipelineEditorPage(),
    ),
    GoRoute(
      path: '/jobs/:id',
      builder: (context, state) {
        final id = state.pathParameters['id']!;
        return JobDetailPage(jobId: id);
      },
    ),
  ],
);

class StralisApp extends StatelessWidget {
  const StralisApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp.router(
      title: 'Stralis',
      theme: ThemeData(
        colorScheme: ColorScheme.fromSeed(seedColor: Colors.blueGrey),
        useMaterial3: true,
      ),
      routerConfig: _router,
    );
  }
}
