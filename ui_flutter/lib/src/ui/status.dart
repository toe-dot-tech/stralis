import 'package:flutter/material.dart';

/// Canonical color for a Stralis job/node status.
Color statusColor(String s) {
  switch (s.toLowerCase()) {
    case 'succeeded':
      return Colors.green;
    case 'running':
      return Colors.blue;
    case 'ready':
    case 'pending':
      return Colors.orange;
    case 'failed':
      return Colors.red;
    case 'skipped':
    case 'cancelled':
      return Colors.grey;
    default:
      return Colors.blueGrey;
  }
}
