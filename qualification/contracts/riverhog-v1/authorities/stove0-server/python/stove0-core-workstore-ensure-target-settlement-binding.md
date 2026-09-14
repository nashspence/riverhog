# stove0_core.WorkStore.ensure_target_settlement_binding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-workstore-ensure-target-settl-93e479683d:3b15b50a20 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-493a1c7162"></a>
- <a id="s-813fc632c7"></a>`distribution`: `stove0-server`
- <a id="s-0169c23bea"></a>`module`: `stove0_core`
- <a id="s-762cb443bd"></a>`name`: `ensure_target_settlement_binding`
- <a id="s-31872906e1"></a>`owner`: `stove0_core.WorkStore`
- <a id="s-644d6eca4a"></a>`unit`: `member`

### Declared structure

- <a id="s-5df124d676"></a>`kind`: `"method"`
- <a id="s-7d39c936ca"></a>`signature`: `"\"(self, record: 'TargetSettlementSealRecord') -> 'TargetSettlementSealRecord'\""`

## Maintained corroboration

### Related interface records

- [stove0_core.WorkStore](stove0-core-workstore.md)

## Governing policies

- <a id="pa-23b3c204cf"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.WorkStore.ensure_target_settlement_binding`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c7d2bb9dcada1b95f82937d046d7b9794d740dea87c8f428a5fdfb1a40cd5661 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, record: 'TargetSettlementSealRecord') -> 'TargetSettlementSealRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "ensure_target_settlement_binding",
  "owner": "stove0_core.WorkStore",
  "unit": "member"
}
```
