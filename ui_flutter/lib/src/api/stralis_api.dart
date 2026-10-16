import 'dart:convert';
import 'package:http/http.dart' as http;
import '../models/job.dart';

class StralisApi {
  final String baseUrl;
  final http.Client _client;

  StralisApi({required this.baseUrl}) : _client = http.Client();

  Future<List<Job>> listJobs({int limit = 20, int offset = 0}) async {
    final uri = Uri.parse('$baseUrl/api/v1/jobs?limit=$limit&offset=$offset');
    final res = await _client.get(uri);
    if (res.statusCode != 200) throw Exception('listJobs failed: ${res.statusCode}');
    final body = jsonDecode(res.body) as List;
    return body.map((e) => Job.fromJson(e as Map<String, dynamic>)).toList();
  }

  Future<JobDetail> getJob(String jobId) async {
    final uri = Uri.parse('$baseUrl/api/v1/jobs/$jobId');
    final res = await _client.get(uri);
    if (res.statusCode != 200) throw Exception('getJob failed: ${res.statusCode}');
    final body = jsonDecode(res.body) as Map<String, dynamic>;
    return JobDetail(
      job: Job.fromJson(body['job'] as Map<String, dynamic>),
      nodeRuns: (body['node_runs'] as List)
          .map((e) => NodeRun.fromJson(e as Map<String, dynamic>))
          .toList(),
    );
  }

  Future<String> submitPipeline(Map<String, dynamic> spec) async {
    final uri = Uri.parse('$baseUrl/api/v1/pipelines');
    final res = await _client.post(
      uri,
      headers: {'Content-Type': 'application/json'},
      body: jsonEncode(spec),
    );
    if (res.statusCode != 202) throw Exception('submit failed: ${res.statusCode} ${res.body}');
    final body = jsonDecode(res.body) as Map<String, dynamic>;
    return body['job_id'] as String;
  }

  Future<Job> retryJob(String jobId) async {
    final uri = Uri.parse('$baseUrl/api/v1/jobs/$jobId/retry');
    final res = await _client.post(uri);
    if (res.statusCode != 202) throw Exception('retry failed: ${res.statusCode}');
    final body = jsonDecode(res.body) as Map<String, dynamic>;
    return Job.fromJson(body);
  }

  void close() => _client.close();
}

class JobDetail {
  final Job job;
  final List<NodeRun> nodeRuns;
  const JobDetail({required this.job, required this.nodeRuns});
}
