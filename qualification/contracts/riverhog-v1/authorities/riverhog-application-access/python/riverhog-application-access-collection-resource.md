# riverhog_application_access.collection_resource

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-collection-resource:d497f40283 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-836f532e72"></a>
| Field | Shape |
|---|---|
| <a id="s-94b72cf10e"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-518ff3e768"></a>`distribution` | "riverhog-application-access" |
| <a id="s-78ed1e44a3"></a>`module` | "riverhog_application_access" |
| <a id="s-423479ca4e"></a>`name` | "collection_resource" |
| <a id="s-4c27097109"></a>`unit` | "export" |

## Governing policies

- <a id="pa-e8e3d013bd"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources.md#src-9d9ce5fdac) — `packages/riverhog-application-access/src/riverhog_application_access/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_application_access.collection_resource`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f8c9e57132755c23fefc7242d777474c6a1c060d4378f5133ce0fe646de9b34d -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(collection_id: 'int | str') -> 'str'\""
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "collection_resource",
  "unit": "export"
}
```
