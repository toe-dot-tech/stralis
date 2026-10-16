// GENERATED CODE - DO NOT MODIFY BY HAND

part of 'job.dart';

// **************************************************************************
// JsonSerializableGenerator
// **************************************************************************

Job _$JobFromJson(Map<String, dynamic> json) => Job(
      id: json['id'] as String,
      name: json['name'] as String,
      status: json['status'] as String,
      error: json['error'] as String?,
      createdAt: DateTime.parse(json['created_at'] as String),
      updatedAt: DateTime.parse(json['updated_at'] as String),
      spec: json['spec'] as Map<String, dynamic>?,
    );

Map<String, dynamic> _$JobToJson(Job instance) => <String, dynamic>{
      'id': instance.id,
      'name': instance.name,
      'status': instance.status,
      'error': instance.error,
      'created_at': instance.createdAt.toIso8601String(),
      'updated_at': instance.updatedAt.toIso8601String(),
      'spec': instance.spec,
    };

NodeRun _$NodeRunFromJson(Map<String, dynamic> json) => NodeRun(
      jobId: json['job_id'] as String,
      nodeId: json['node_id'] as String,
      handler: json['handler'] as String,
      status: json['status'] as String,
      attempt: (json['attempt'] as num).toInt(),
      maxAttempts: (json['max_attempts'] as num).toInt(),
      nodeHash: json['node_hash'] as String?,
      outputs: json['outputs'] as Map<String, dynamic>? ?? const {},
      error: json['error'] as String?,
    );

Map<String, dynamic> _$NodeRunToJson(NodeRun instance) => <String, dynamic>{
      'job_id': instance.jobId,
      'node_id': instance.nodeId,
      'handler': instance.handler,
      'status': instance.status,
      'attempt': instance.attempt,
      'max_attempts': instance.maxAttempts,
      'node_hash': instance.nodeHash,
      'outputs': instance.outputs,
      'error': instance.error,
    };
