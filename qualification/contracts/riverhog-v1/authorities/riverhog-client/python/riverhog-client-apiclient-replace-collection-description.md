# riverhog_client.ApiClient.replace_collection_description

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-replace-collect-9aa8fc3ac0:83b9e53422 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-705888d839"></a>
| Field | Shape |
|---|---|
| <a id="s-b78054b6f0"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-aa2517e70a"></a>`distribution` | "riverhog-client" |
| <a id="s-57a99362ea"></a>`module` | "riverhog_client" |
| <a id="s-0e096ffad1"></a>`name` | "replace_collection_description" |
| <a id="s-1929d86ebd"></a>`owner` | "riverhog_client.ApiClient" |
| <a id="s-6d633ec218"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_client.ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-3f4d4c323c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.replace_collection_description`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1c20cf6222a347d75cf20e743aea1ffe515836c9ff9f17c5f809e4da961aeb63 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', description: 'CollectionDescription | None', *, expected_identity: 'str') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "replace_collection_description",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```
