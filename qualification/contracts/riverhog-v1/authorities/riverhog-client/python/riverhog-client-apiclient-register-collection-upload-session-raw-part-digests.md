# riverhog_client.ApiClient.register_collection_upload_session_raw_part_digests

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-register-collec-ac44800b20:e8114f31fa -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2631c7b201"></a>
- <a id="s-be28fd10f5"></a>`distribution`: `riverhog-client`
- <a id="s-638d1e84de"></a>`module`: `riverhog_client`
- <a id="s-f8ac9599a5"></a>`name`: `register_collection_upload_session_raw_part_digests`
- <a id="s-5bd68eaed3"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-56b97ef062"></a>`unit`: `member`

### Declared structure

- <a id="s-09c6c42bd8"></a>`kind`: `"method"`
- <a id="s-ce220afa68"></a>`signature`: `"\"(self, collection_id: 'CollectionId', batch: 'CollectionUploadRawDigestBatchDocument \| Mapping[str, Any]') -> 'CollectionUploadRawDigestProgressDocument'\""`

## Maintained corroboration

### Related interface records

- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-48266c0f2e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.register_collection_upload_session_raw_part_digests`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1c92bcb14bae0920fee08e6295e794f0f8d74f51814945c5887acc96eb525e56 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', batch: 'CollectionUploadRawDigestBatchDocument | Mapping[str, Any]') -> 'CollectionUploadRawDigestProgressDocument'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "register_collection_upload_session_raw_part_digests",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```
