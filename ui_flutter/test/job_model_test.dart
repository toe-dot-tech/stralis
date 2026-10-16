import 'package:flutter_test/flutter_test.dart';
import 'package:stralis_ui/src/models/job.dart';

void main() {
  test('Job.fromJson parses the spec and timestamps', () {
    final job = Job.fromJson(const {
      'id': 'abc',
      'name': 'demo',
      'status': 'running',
      'error': null,
      'created_at': '2024-01-01T00:00:00Z',
      'updated_at': '2024-01-01T00:00:01Z',
      'spec': {
        'name': 'demo',
        'nodes': [
          {'id': 'a', 'handler': 'noop', 'depends_on': <String>[]},
        ],
      },
    });

    expect(job.id, 'abc');
    expect(job.status, 'running');
    expect(job.spec!['nodes'], hasLength(1));
    expect(job.createdAt.isUtc, isTrue);
  });

  test('Job.fromJson tolerates a missing spec', () {
    final job = Job.fromJson(const {
      'id': 'abc',
      'name': 'demo',
      'status': 'pending',
      'created_at': '2024-01-01T00:00:00Z',
      'updated_at': '2024-01-01T00:00:01Z',
    });
    expect(job.spec, isNull);
  });

  test('NodeRun.fromJson defaults missing outputs to an empty map', () {
    final run = NodeRun.fromJson(const {
      'job_id': 'abc',
      'node_id': 'a',
      'handler': 'noop',
      'status': 'succeeded',
      'attempt': 1,
      'max_attempts': 3,
      'node_hash': null,
      'outputs': null,
      'error': null,
    });

    expect(run.outputs, isEmpty);
    expect(run.nodeId, 'a');
    expect(run.status, 'succeeded');
  });
}
