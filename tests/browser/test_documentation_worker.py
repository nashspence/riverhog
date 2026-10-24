from __future__ import annotations

from pathlib import Path

from playwright.sync_api import Browser

ROOT = Path(__file__).resolve().parents[2]
SOURCE = (ROOT / "scripts/contract_atlas/documentation_worker.js").read_text()


def evaluate(browser: Browser, script: str):
    page = browser.new_page()
    try:
        return page.evaluate(
            "async ({source, test}) => {"
            "const createWorker = new Function(source.replace('export function', 'function') + "
            "';return createWorker')();"
            "return await new Function('createWorker', test)(createWorker);"
            "}",
            {"source": SOURCE, "test": script},
        )
    finally:
        page.close()


def test_worker_preserves_paths_mime_conditionals_and_read_only_authority(browser):
    result = evaluate(
        browser,
        """return (async () => {
          const digest = "a".repeat(64);
          const worker = createWorker([
            ["index.html", [digest, 5, "text/html; charset=utf-8"]],
            ["folder/index.html", [digest, 5, "text/html; charset=utf-8"]],
            ["schema.json", [digest, 5, "application/json"]],
          ]);
          const env = {SITE_MOUNT: "/riverhog/", CONTENT_BASE_URL: "https://storage.example/objects/"};
          const original = globalThis.fetch;
          const calls = [];
          globalThis.fetch = async (url, options) => {
            calls.push({url, method: options.method, headers: options.headers});
            return new Response(options.method === "HEAD" ? null : "exact", {
              headers: {
                "Content-Length": "5", "X-Bz-Private": "not forwarded",
                "Set-Cookie": "not forwarded"
              }
            });
          };
          try {
            const request = (path, options) => worker.fetch(
              new Request("https://docs.example" + path, options), env);
            const body = await request("/riverhog/?docs=1");
            const text = await body.text();
            const head = await request("/riverhog/schema.json", {method: "HEAD"});
            const conditional = await request("/riverhog/schema.json", {
              headers: {"If-None-Match": 'W/"' + digest + '"'}
            });
            const beforeInvalid = calls.length;
            const invalid = await Promise.all([
              request("/riverhog/missing"),
              request("/other/schema.json"),
              request("/riverhog/%ZZ"),
              request("/riverhog/%2e%2e%2fsecret"),
              request("/riverhog/schema.json", {method: "PUT", body: "changed"}),
            ]);
            const folder = await request("/riverhog/folder?docs=1");
            const root = await request("/?docs=1");
            return {
              text,
              type: body.headers.get("Content-Type"),
              etag: body.headers.get("ETag"),
              privateHeader: body.headers.get("X-Bz-Private"),
              cookie: body.headers.get("Set-Cookie"),
              headBody: await head.text(),
              headLength: head.headers.get("Content-Length"),
              conditional: conditional.status,
              invalid: invalid.map(response => response.status),
              invalidDidNotFetch: calls.length === beforeInvalid,
              folder: folder.headers.get("Location"),
              root: root.headers.get("Location"),
              calls,
            };
          } finally { globalThis.fetch = original; }
        })();""",
    )
    assert result["text"] == "exact"
    assert result["type"] == "text/html; charset=utf-8"
    assert result["etag"] == '"' + "a" * 64 + '"'
    assert result["privateHeader"] is None and result["cookie"] is None
    assert result["headBody"] == "" and result["headLength"] == "5"
    assert result["conditional"] == 304
    assert result["invalid"] == [404, 404, 400, 400, 405]
    assert result["invalidDidNotFetch"] is True
    assert result["folder"] == "https://docs.example/riverhog/folder/?docs=1"
    assert result["root"] == "https://docs.example/riverhog/?docs=1"
    assert len(result["calls"]) == 2
    assert all(
        call["url"] == "https://storage.example/objects/" + "a" * 64 for call in result["calls"]
    )
    assert all(
        call["headers"]["User-Agent"] == "riverhog-documentation/1.0" for call in result["calls"]
    )


def test_worker_streams_large_content_and_fails_closed_when_verified_storage_is_unavailable(
    browser,
):
    result = evaluate(
        browser,
        """return (async () => {
          const digest = "b".repeat(64);
          const size = 36298349;
          const worker = createWorker([["large.html", [digest, size, "text/html"]]]);
          const env = {SITE_MOUNT: "/", CONTENT_BASE_URL: "https://storage.example/objects/"};
          const original = globalThis.fetch;
          let produced = 0;
          let chunks = 0;
          const stream = new ReadableStream({
            pull(controller) {
              if (produced === size) { controller.close(); return; }
              const length = Math.min(65536, size - produced);
              produced += length;
              chunks++;
              controller.enqueue(new Uint8Array(length).fill(37));
            },
          });
          globalThis.fetch = async () => new Response(stream, {
            headers: {"Content-Length": String(size)}
          });
          try {
            const response = await worker.fetch(
              new Request("https://docs.example/large.html"), env);
            const beforeRead = produced;
            let received = 0;
            const reader = response.body.getReader();
            for (;;) {
              const {done, value} = await reader.read();
              if (done) break;
              received += value.length;
              if (!value.every(byte => byte === 37)) throw new Error("bytes changed");
            }
            globalThis.fetch = async () => new Response("missing", {status: 404});
            const missing = await worker.fetch(new Request("https://docs.example/large.html"), env);
            globalThis.fetch = async () => { throw new Error("private origin details"); };
            const failed = await worker.fetch(new Request("https://docs.example/large.html"), env);
            return {
              status: response.status, beforeRead, received, chunks,
              missing: missing.status, failed: failed.status, errorBody: await failed.text()
            };
          } finally { globalThis.fetch = original; }
        })();""",
    )
    assert result["status"] == 200
    assert result["beforeRead"] <= 65536
    assert result["received"] == 36_298_349 and result["chunks"] > 500
    assert result["missing"] == result["failed"] == 502
    assert result["errorBody"] == ""
