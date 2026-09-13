# stove0_core.WorkStore.load

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-workstore-load:0bebe7e68a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-55d254ed94"></a>
| Field | Shape |
|---|---|
| <a id="s-ef4b64b4b9"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-324c4648c9"></a>`distribution` | "stove0-server" |
| <a id="s-b5fe60716b"></a>`module` | "stove0_core" |
| <a id="s-934e7e16ed"></a>`name` | "load" |
| <a id="s-3c2a30229d"></a>`owner` | "stove0_core.WorkStore" |
| <a id="s-21adf6b49e"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_core.WorkStore](stove0-core-workstore.md)

## Governing policies

- <a id="pa-9a5f8502cd"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.WorkStore.load`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e2af139a614a1ffe15dd99773785b36523448491d1cf52c3449823db6c3ea62f -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str') -> 'WorkRecord | None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "load",
  "owner": "stove0_core.WorkStore",
  "unit": "member"
}
```
