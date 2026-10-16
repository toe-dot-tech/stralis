import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:stralis_ui/src/widgets/dag_view.dart';

void main() {
  testWidgets('DagView renders node ids, handlers and live status',
      (tester) async {
    await tester.pumpWidget(
      const MaterialApp(
        home: Scaffold(
          body: DagView(
            nodes: [
              {'id': 'seed', 'handler': 'noop', 'status': 'succeeded'},
              {'id': 'down', 'handler': 'shell', 'status': 'running'},
            ],
            edges: [
              {'from': 'seed', 'to': 'down'},
            ],
          ),
        ),
      ),
    );
    await tester.pump();

    expect(find.text('seed'), findsOneWidget);
    expect(find.text('noop'), findsOneWidget);
    expect(find.text('SUCCEEDED'), findsOneWidget);
    expect(find.text('down'), findsOneWidget);
    expect(find.text('RUNNING'), findsOneWidget);
  });
}
