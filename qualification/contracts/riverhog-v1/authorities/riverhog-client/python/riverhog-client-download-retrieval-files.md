# riverhog_client.download_retrieval_files

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-download-retrieval-files:59e75891a8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8ad8baa981"></a>
- <a id="s-250e12d9b7"></a>`distribution`: `riverhog-client`
- <a id="s-6466fe3c6c"></a>`module`: `riverhog_client`
- <a id="s-d42b7c2fc9"></a>`name`: `download_retrieval_files`
- <a id="s-34c88f7cc0"></a>`unit`: `export`

### Declared structure

- <a id="s-d96a9402d8"></a>`kind`: `"function"`
- <a id="s-5e581618c5"></a>`signature`: `"\"(api: 'RetrievalDownloadApi', job_id: 'str', downloads: 'Sequence[RetrievalDownload]', *, concurrency: 'int', window: 'int', client_factory: 'Callable[[], RetrievalDownloadApi] \| None' = None, on_downloaded: 'DownloadProgress \| None' = None, heartbeat: 'DownloadHeartbeat \| None' = None, heartbeat_interval_seconds: 'float' = 60.0) -> 'int'\""`

## Governing policies

- <a id="pa-aeb5711fe5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.download_retrieval_files`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 911641144a9e513606a93898327cb9b5cd300d61c05067512a3ff8ab932fee4a -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(api: 'RetrievalDownloadApi', job_id: 'str', downloads: 'Sequence[RetrievalDownload]', *, concurrency: 'int', window: 'int', client_factory: 'Callable[[], RetrievalDownloadApi] | None' = None, on_downloaded: 'DownloadProgress | None' = None, heartbeat: 'DownloadHeartbeat | None' = None, heartbeat_interval_seconds: 'float' = 60.0) -> 'int'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "download_retrieval_files",
  "unit": "export"
}
```
