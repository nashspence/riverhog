# stove0_core.SqlAlchemyStateStore.ensure_target_settlement_binding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-sqlalchemystatestore-ensure-t-e475fdafc6:5adea0cb03 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6ce0fd0e9d"></a>
- <a id="s-c713e4d44f"></a>`distribution`: `stove0-server`
- <a id="s-d764b7ce80"></a>`module`: `stove0_core`
- <a id="s-de9b304c6f"></a>`name`: `ensure_target_settlement_binding`
- <a id="s-d452240da2"></a>`owner`: `stove0_core.SqlAlchemyStateStore`
- <a id="s-8798669bb4"></a>`unit`: `member`

### Declared structure

- <a id="s-530cf48897"></a>`kind`: `"method"`
- <a id="s-3d22d22475"></a>`signature`: `"\"(self, record: 'TargetSettlementSealRecord') -> 'TargetSettlementSealRecord'\""`

## Maintained corroboration

### Related interface records

- [SqlAlchemyStateStore](stove0-core-sqlalchemystatestore.md)

## Governing policies

- <a id="pa-eeed2fd157"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.SqlAlchemyStateStore.ensure_target_settlement_binding`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 715558eaabf69e7c614fbde4dfe5a04063218f839231be3179bf7692ce9dd9d9 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, record: 'TargetSettlementSealRecord') -> 'TargetSettlementSealRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "ensure_target_settlement_binding",
  "owner": "stove0_core.SqlAlchemyStateStore",
  "unit": "member"
}
```
