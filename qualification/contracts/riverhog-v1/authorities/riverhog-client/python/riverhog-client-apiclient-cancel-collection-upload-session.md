# riverhog_client.ApiClient.cancel_collection_upload_session

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-cancel-collecti-33ced8420a:59ce5a7add -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3fd44f9082"></a>
| Field | Shape |
|---|---|
| <a id="s-b5d77faa02"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-0ace0346a4"></a>`distribution` | "riverhog-client" |
| <a id="s-b7147939c1"></a>`module` | "riverhog_client" |
| <a id="s-6479e9467e"></a>`name` | "cancel_collection_upload_session" |
| <a id="s-4c8d045554"></a>`owner` | "riverhog_client.ApiClient" |
| <a id="s-e9015a7caf"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_client.ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-9a657a5af1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.cancel_collection_upload_session`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3a2126c6784a8e221c6d82bc9d2a248739410f4488664e48ce0e0261cb622884 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "cancel_collection_upload_session",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```
