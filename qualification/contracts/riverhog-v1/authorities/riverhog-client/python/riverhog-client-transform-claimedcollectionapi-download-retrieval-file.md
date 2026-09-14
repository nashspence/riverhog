# riverhog_client.transform.ClaimedCollectionApi.download_retrieval_file

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-claimedcollecti-5562b8163a:2bb8f59570 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-af78f7e3fb"></a>
- <a id="s-c2dc162636"></a>`distribution`: `riverhog-client`
- <a id="s-be955ea80e"></a>`module`: `riverhog_client.transform`
- <a id="s-6e1ffc8328"></a>`name`: `download_retrieval_file`
- <a id="s-35ae52d3d0"></a>`owner`: `riverhog_client.transform.ClaimedCollectionApi`
- <a id="s-e141425a2d"></a>`unit`: `member`

### Declared structure

- <a id="s-ac93e75738"></a>`kind`: `"method"`
- <a id="s-24e70fcd38"></a>`signature`: `"\"(self, job_id: 'str', *, collection_id: 'CollectionId', path: 'str', output: 'Path', expected_bytes: 'int', expected_sha256: 'str') -> 'int'\""`

## Maintained corroboration

### Related interface records

- [riverhog_client.transform.ClaimedCollectionApi](riverhog-client-transform-claimedcollectionapi.md)

## Governing policies

- <a id="pa-5173f16c5e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.ClaimedCollectionApi.download_retrieval_file`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d4d7bacddc16cd2f1cfeae0b3475e078c440cfa85dc2f9e27ba5bec9358839eb -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, job_id: 'str', *, collection_id: 'CollectionId', path: 'str', output: 'Path', expected_bytes: 'int', expected_sha256: 'str') -> 'int'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "download_retrieval_file",
  "owner": "riverhog_client.transform.ClaimedCollectionApi",
  "unit": "member"
}
```
