"""Tests for the localhost-only `/metrics` HTTP server."""

from __future__ import annotations

import http.client

import pytest

from reroll_sync.metrics_server import MetricsServer


@pytest.fixture
def server():
    created: list[MetricsServer] = []

    def _make(render):
        server = MetricsServer(0, render)
        server.start()
        created.append(server)
        return server

    yield _make
    for server in created:
        server.stop()


def _get(server: MetricsServer, path: str) -> http.client.HTTPResponse:
    conn = http.client.HTTPConnection("127.0.0.1", server.port(), timeout=5)
    try:
        conn.request("GET", path)
        return conn.getresponse()
    finally:
        conn.close()


def test_metrics_path_returns_the_rendered_text(server):
    s = server(lambda: "reroll_sync_wal_bytes 1000\n")
    response = _get(s, "/metrics")
    body = response.read().decode("utf-8")
    assert response.status == 200
    assert body == "reroll_sync_wal_bytes 1000\n"


def test_metrics_path_sets_the_prometheus_content_type(server):
    s = server(lambda: "x 1\n")
    response = _get(s, "/metrics")
    response.read()
    assert response.getheader("Content-Type") == "text/plain; version=0.0.4; charset=utf-8"


def test_render_is_called_fresh_on_every_request(server):
    calls = {"n": 0}

    def _render() -> str:
        calls["n"] += 1
        return f"reroll_sync_calls {calls['n']}\n"

    s = server(_render)
    first = _get(s, "/metrics").read().decode()
    second = _get(s, "/metrics").read().decode()
    assert first == "reroll_sync_calls 1\n"
    assert second == "reroll_sync_calls 2\n"


def test_unknown_path_returns_404(server):
    s = server(lambda: "x 1\n")
    response = _get(s, "/unknown")
    response.read()
    assert response.status == 404


def test_server_binds_only_to_localhost(server):
    s = server(lambda: "x 1\n")
    assert s._server.server_address[0] == "127.0.0.1"


def test_stop_without_start_is_a_noop():
    server = MetricsServer(0, lambda: "x 1\n")
    server.stop()


def test_port_reflects_the_os_assigned_port_when_constructed_with_zero(server):
    s = server(lambda: "x 1\n")
    assert s.port() != 0


def test_render_raising_returns_500_with_a_short_error_body(server):
    def _boom() -> str:
        raise RuntimeError("boom")

    s = server(_boom)
    response = _get(s, "/metrics")
    body = response.read()
    assert response.status == 500
    assert body


def test_server_survives_a_render_failure_for_the_next_request(server):
    calls = {"n": 0}

    def _render() -> str:
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("boom")
        return "reroll_sync_ok 1\n"

    s = server(_render)

    first = _get(s, "/metrics")
    first.read()
    assert first.status == 500

    second = _get(s, "/metrics")
    assert second.status == 200
    assert second.read().decode() == "reroll_sync_ok 1\n"
