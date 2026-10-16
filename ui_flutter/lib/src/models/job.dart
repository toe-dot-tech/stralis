import 'package:equatable/equatable.dart';
import 'package:json_annotation/json_annotation.dart';

part 'job.g.dart';

@JsonSerializable()
class Job extends Equatable {
  final String id;
  final String name;
  final String status;
  final String? error;
  @JsonKey(name: 'created_at')
  final DateTime createdAt;
  @JsonKey(name: 'updated_at')
  final DateTime updatedAt;

  const Job({
    required this.id,
    required this.name,
    required this.status,
    this.error,
    required this.createdAt,
    required this.updatedAt,
  });

  factory Job.fromJson(Map<String, dynamic> json) => _$JobFromJson(json);
  Map<String, dynamic> toJson() => _$JobToJson(this);

  @override
  List<Object?> get props => [id, name, status, error, createdAt, updatedAt];
}

@JsonSerializable()
class NodeRun extends Equatable {
  @JsonKey(name: 'job_id')
  final String jobId;
  @JsonKey(name: 'node_id')
  final String nodeId;
  final String handler;
  final String status;
  final int attempt;
  @JsonKey(name: 'max_attempts')
  final int maxAttempts;
  @JsonKey(name: 'node_hash')
  final String? nodeHash;
  final Map<String, dynamic> outputs;
  final String? error;

  const NodeRun({
    required this.jobId,
    required this.nodeId,
    required this.handler,
    required this.status,
    required this.attempt,
    required this.maxAttempts,
    this.nodeHash,
    this.outputs = const {},
    this.error,
  });

  factory NodeRun.fromJson(Map<String, dynamic> json) => _$NodeRunFromJson(json);
  Map<String, dynamic> toJson() => _$NodeRunToJson(this);

  @override
  List<Object?> get props => [jobId, nodeId, handler, status, attempt, maxAttempts, nodeHash, outputs, error];
}
