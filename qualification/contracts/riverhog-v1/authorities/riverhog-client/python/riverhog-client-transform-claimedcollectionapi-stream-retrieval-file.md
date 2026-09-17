# riverhog_client.transform.ClaimedCollectionApi.stream_retrieval_file

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-claimedcollecti-cd2815d10e:d837e21bff -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3c2be362c2"></a>
- <a id="s-d9ac4f5bff"></a>`distribution`: `riverhog-client`
- <a id="s-a6bf8f9ed6"></a>`module`: `riverhog_client.transform`
- <a id="s-b34b87fd43"></a>`name`: `stream_retrieval_file`
- <a id="s-1d919ab048"></a>`owner`: `riverhog_client.transform.ClaimedCollectionApi`
- <a id="s-445d8e06a7"></a>`unit`: `member`

### Declared structure

- <a id="s-3d64b9cc81"></a>`kind`: `"method"`
- <a id="s-bbf27be1b8"></a>`signature`: `"\"(self, job_id: 'str', *, collection_id: 'CollectionId', path: 'str', expected_bytes: 'int', expected_sha256: 'str', start: 'int' = 0, end: 'int \| None' = None, chunk_size: 'int' = 8388608) -> 'AbstractContextManager[Iterator[bytes]]'\""`

## Maintained corroboration

### Related interface records

- [ClaimedCollectionApi](riverhog-client-transform-claimedcollectionapi.md)

## Governing policies

- <a id="pa-71bafec8fd"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — [packages/riverhog-client/src/riverhog\_client/transform/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/transform/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.transform.ClaimedCollectionApi.stream_retrieval_file`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b92777b30622a0bdf322a86deac3730343e52e9793552eef8e2a89bb8ba4a8b3 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, job_id: 'str', *, collection_id: 'CollectionId', path: 'str', expected_bytes: 'int', expected_sha256: 'str', start: 'int' = 0, end: 'int | None' = None, chunk_size: 'int' = 8388608) -> 'AbstractContextManager[Iterator[bytes]]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "stream_retrieval_file",
  "owner": "riverhog_client.transform.ClaimedCollectionApi",
  "unit": "member"
}
```

</details>
