# riverhog_client.ApiClient.download_retrieval_file

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-download-retrieval-file:4c903658ff -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-441ceb2550"></a>
- <a id="s-fbb09413df"></a>`distribution`: `riverhog-client`
- <a id="s-963e6e3b25"></a>`module`: `riverhog_client`
- <a id="s-6cf4941c2b"></a>`name`: `download_retrieval_file`
- <a id="s-be8dd1b80e"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-5df55ffbfa"></a>`unit`: `member`

### Declared structure

- <a id="s-1b876e5364"></a>`kind`: `"method"`
- <a id="s-7be726f270"></a>`signature`: `"\"(self, job_id: 'str', *, collection_id: 'CollectionId', path: 'str', output: 'Path', expected_bytes: 'int', expected_sha256: 'str', progress: 'DownloadProgress \| None' = None) -> 'int'\""`

## Maintained corroboration

### Related interface records

- [piggity local repair](../../piggity/cli/piggity-local-repair.md)
- [piggity local sync](../../piggity/cli/piggity-local-sync.md)
- [GET /v1/retrieval-jobs/{job_id}/content](../../riverhog/http-operations/get-v1-retrieval-jobs-job-id-content.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-18689a68d0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.download\_retrieval\_file](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L971)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.download_retrieval_file`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 39df2044653b9ffa49001cac525f6f5c01ab2c8d9777dd199b42f77cc31aca6c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, job_id: 'str', *, collection_id: 'CollectionId', path: 'str', output: 'Path', expected_bytes: 'int', expected_sha256: 'str', progress: 'DownloadProgress | None' = None) -> 'int'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "download_retrieval_file",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
