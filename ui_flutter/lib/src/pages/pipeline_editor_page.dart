import 'dart:convert';
import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:go_router/go_router.dart';
import 'dashboard_page.dart';

class PipelineEditorPage extends ConsumerStatefulWidget {
  const PipelineEditorPage({super.key});

  @override
  ConsumerState<PipelineEditorPage> createState() => _PipelineEditorPageState();
}

class _PipelineEditorPageState extends ConsumerState<PipelineEditorPage> {
  final _controller = TextEditingController(text: jsonEncode({
    "name": "demo-diamond",
    "nodes": [
      {"id": "seed", "handler": "noop", "params": {"outputs": ["out"]}},
      {"id": "down", "handler": "noop", "params": {}, "depends_on": ["seed"]}
    ]
  }, indent: 2));

  bool _submitting = false;
  String? _error;

  @override
  void dispose() {
    _controller.dispose();
    super.dispose();
  }

  Future<void> _submit() async {
    setState(() { _submitting = true; _error = null; });
    try {
      final spec = jsonDecode(_controller.text) as Map<String, dynamic>;
      await ref.read(apiProvider).submitPipeline(spec);
      if (mounted) context.go('/');
      ref.invalidate(jobsProvider);
    } catch (e) {
      setState(() { _error = e.toString(); });
    } finally {
      if (mounted) setState(() { _submitting = false; });
    }
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('Pipeline Editor'),
        actions: [
          IconButton(
            icon: _submitting ? const CircularProgressIndicator(color: Colors.white) : const Icon(Icons.send),
            onPressed: _submitting ? null : _submit,
            tooltip: 'Submit',
          )
        ],
      ),
      body: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text('Paste PipelineSpec JSON', style: Theme.of(context).textTheme.titleSmall),
            const SizedBox(height: 8),
            Expanded(
              child: TextField(
                controller: _controller,
                maxLines: null,
                expands: true,
                decoration: const InputDecoration(border: OutlineInputBorder()),
                style: const TextStyle(fontFamily: 'monospace'),
              ),
            ),
            if (_error != null) ...[
              const SizedBox(height: 8),
              Text(_error!, style: TextStyle(color: Theme.of(context).colorScheme.error)),
            ]
          ],
        ),
      ),
    );
  }
}
