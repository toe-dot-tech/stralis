# Stralis Flutter UI

Flutter (Dart) client for the Stralis control plane — a single codebase targeting **web**, **desktop**
and **mobile**. It talks only to the public HTTP API, so it works against any Stralis deployment,
including the zero-services local mode.

## Features

- **Dashboard** (`/`) — recent jobs with status chips; auto-refreshes every 3s while jobs are live.
- **Job detail** (`/jobs/:id`) — an interactive, pan/zoom **DAG** rendered from the job's `PipelineSpec`
  with nodes colored by live `NodeRun` status, plus a per-node run table (attempts, handler, errors) and
  a **Retry** action for failed jobs. Polling stops once the job is terminal.
- **Pipeline editor** (`/editor`) — paste a `PipelineSpec` JSON document and submit it.

Stack: Riverpod (state) · `go_router` (routing) · `graphview` (Buchheim–Walker DAG layout) ·
`json_serializable` (typed models).

## Getting started

1. Install the Flutter SDK (Dart `>=3.8.0`).
2. Fetch packages and generate the model code:

   ```bash
   cd ui_flutter
   flutter pub get
   dart run build_runner build --delete-conflicting-outputs   # writes src/models/*.g.dart
   ```

3. Start a Stralis API (see the root `README.md`). The app defaults to `http://127.0.0.1:8100`; override
   it at build/run time with `--dart-define`:

   ```bash
   flutter run -d chrome --dart-define=STRALIS_API_URL=http://127.0.0.1:8100
   ```

4. Tests and static analysis:

   ```bash
   flutter test
   flutter analyze
   ```

## CORS

The API sends permissive CORS headers by default, so browser development works out of the box. Lock this
down with `STRALIS_CORS_ORIGINS` (comma-separated) before exposing the API. No auth is implemented — run
the API behind your own gateway in any untrusted environment.
