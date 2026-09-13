# stove0_core.WorkRecord.work_id

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-workrecord-work-id:63fdd1ba78 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5bc59a4308"></a>
| Field | Shape |
|---|---|
| <a id="s-fbe6858165"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-5676fe28ad"></a>`distribution` | "stove0-server" |
| <a id="s-be6d4fde1e"></a>`module` | "stove0_core" |
| <a id="s-31cf6f4f77"></a>`name` | "work_id" |
| <a id="s-33da1f5131"></a>`owner` | "stove0_core.WorkRecord" |
| <a id="s-3a38465acf"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_core.WorkRecord](stove0-core-workrecord.md)

## Governing policies

- <a id="pa-31b0868056"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.WorkRecord.work_id`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 89fd75cceb815d2a4cb13d251370a311ea52123ad7d3e0e813c5b449f6dd7b4c -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> 'str'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "work_id",
  "owner": "stove0_core.WorkRecord",
  "unit": "member"
}
```
