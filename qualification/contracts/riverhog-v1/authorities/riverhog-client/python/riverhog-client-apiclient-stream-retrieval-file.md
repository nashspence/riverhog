# riverhog_client.ApiClient.stream_retrieval_file

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-stream-retrieval-file:5b81cff223 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-10e27c5822"></a>
- <a id="s-64d07e634a"></a>`distribution`: `riverhog-client`
- <a id="s-dcd0f732ca"></a>`module`: `riverhog_client`
- <a id="s-4e010c6359"></a>`name`: `stream_retrieval_file`
- <a id="s-13bedae82f"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-43ffb68bcd"></a>`unit`: `member`

### Declared structure

- <a id="s-52f6e84121"></a>`kind`: `"method"`
- <a id="s-cb63031f03"></a>`signature`: `"\"(self, job_id: 'str', *, collection_id: 'CollectionId', path: 'str', expected_bytes: 'int', expected_sha256: 'str', start: 'int' = 0, end: 'int \| None' = None, chunk_size: 'int' = 8388608) -> 'Iterator[Iterator[bytes]]'\""`

## Maintained corroboration

### Related interface records

- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-18ffb57dca"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.stream_retrieval_file`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 30e0ec33d25784bad5756a5bf1670a4d4e6f0b66d09b63bb6406b76dc34cd0c4 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, job_id: 'str', *, collection_id: 'CollectionId', path: 'str', expected_bytes: 'int', expected_sha256: 'str', start: 'int' = 0, end: 'int | None' = None, chunk_size: 'int' = 8388608) -> 'Iterator[Iterator[bytes]]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "stream_retrieval_file",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
