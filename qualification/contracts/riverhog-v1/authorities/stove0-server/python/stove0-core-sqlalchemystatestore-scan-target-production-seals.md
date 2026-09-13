# stove0_core.SqlAlchemyStateStore.scan_target_production_seals

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-sqlalchemystatestore-scan-tar-a168059219:bfc47c20f6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b0d1a88ec7"></a>
| Field | Shape |
|---|---|
| <a id="s-bafccaec46"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-748e5720c3"></a>`distribution` | "stove0-server" |
| <a id="s-7b9ddcc891"></a>`module` | "stove0_core" |
| <a id="s-9ac8b35f4a"></a>`name` | "scan_target_production_seals" |
| <a id="s-ca61e09a65"></a>`owner` | "stove0_core.SqlAlchemyStateStore" |
| <a id="s-7ed31a2d9b"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_core.SqlAlchemyStateStore](stove0-core-sqlalchemystatestore.md)

## Governing policies

- <a id="pa-1064bc39e1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.SqlAlchemyStateStore.scan_target_production_seals`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8b5507455a11c5ca0e45d29f46896be706933bd319e562a35594c0359011b981 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, state: 'TargetProductionSealState', limit: 'int') -> 'tuple[TargetProductionSealRecord, ...]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "scan_target_production_seals",
  "owner": "stove0_core.SqlAlchemyStateStore",
  "unit": "member"
}
```
