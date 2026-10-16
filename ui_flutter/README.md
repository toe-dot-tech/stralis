# Stralis Flutter UI

Minimal Flutter client for the Stralis API (list jobs, view job details, submit pipelines).

## Getting started

1. Ensure Flutter SDK (>=3.3.0) is installed.
2. From `ui_flutter/`: `flutter pub get`
3. Run against a running Stralis API. Set `STRALIS_API_URL` via `--dart-define` or edit default in `dashboard_page.dart` (default `http://127.0.0.1:8100`).
4. Launch: `flutter run -d chrome` (web) or `flutter run -d windows` (desktop).

The UI talks to the same REST API used by CLI/curl: submit (POST `/api/v1/pipelines` returns 202), list jobs, get job detail, retry. No auth, CORS depends on your uvicorn setup (use `--cors-origins` or run behind proxy if needed).

## Features

- Dashboard: recent jobs with status chips + refresh
- Job detail: node runs, attempts, handler, status
- Pipeline editor: paste JSON PipelineSpec and submit
- Riverpod + go_router, typed models with json_serializable