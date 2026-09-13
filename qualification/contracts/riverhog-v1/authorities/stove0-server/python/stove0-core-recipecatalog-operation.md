# stove0_core.RecipeCatalog.operation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-recipecatalog-operation:f82ef0c004 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-882682945c"></a>
| Field | Shape |
|---|---|
| <a id="s-7eacc91c08"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-f6fcf39449"></a>`distribution` | "stove0-server" |
| <a id="s-aee17a8796"></a>`module` | "stove0_core" |
| <a id="s-e19270d636"></a>`name` | "operation" |
| <a id="s-da14279414"></a>`owner` | "stove0_core.RecipeCatalog" |
| <a id="s-79d36355bb"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_core.RecipeCatalog](stove0-core-recipecatalog.md)

## Governing policies

- <a id="pa-184f13cdde"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.RecipeCatalog.operation`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: df457f3faf31586cee6fda5b9e5138f1aa2160bc081475ccabf36e3990bb0dbd -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, operation_id: 'str') -> 'OperationContract'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "operation",
  "owner": "stove0_core.RecipeCatalog",
  "unit": "member"
}
```
