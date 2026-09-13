# riverhog_client.ApiClient.list_collection_upload_session_files

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-list-collection-5bf87239b0:2688e408f9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8cfb3d707b"></a>
| Field | Shape |
|---|---|
| <a id="s-4c788f85d2"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-9c9801c7e3"></a>`distribution` | "riverhog-client" |
| <a id="s-d03a0a5f7d"></a>`module` | "riverhog_client" |
| <a id="s-c9df6a8e9a"></a>`name` | "list_collection_upload_session_files" |
| <a id="s-67ff55361f"></a>`owner` | "riverhog_client.ApiClient" |
| <a id="s-c47e9843ac"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_client.ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-1974a6905d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.list_collection_upload_session_files`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 927821cdcd4b03623002713bf085b0c6cf37d656c372e89d03057f49612e2114 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', *, page_size: 'int' = 25, page_token: 'str | None' = None) -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "list_collection_upload_session_files",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```
