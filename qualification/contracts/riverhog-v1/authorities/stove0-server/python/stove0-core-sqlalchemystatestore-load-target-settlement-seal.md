# stove0_core.SqlAlchemyStateStore.load_target_settlement_seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-sqlalchemystatestore-load-tar-19f1fba448:313e901781 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f541e76d2a"></a>
- <a id="s-1d5a2c4108"></a>`distribution`: `stove0-server`
- <a id="s-09f12f2ad7"></a>`module`: `stove0_core`
- <a id="s-92d5e3e4a9"></a>`name`: `load_target_settlement_seal`
- <a id="s-7ae78175b9"></a>`owner`: `stove0_core.SqlAlchemyStateStore`
- <a id="s-7adcf71d33"></a>`unit`: `member`

### Declared structure

- <a id="s-b8ce76171b"></a>`kind`: `"method"`
- <a id="s-2803f5f293"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str') -> 'TargetSettlementSealRecord \| None'\""`

## Maintained corroboration

### Related interface records

- [stove0_core.SqlAlchemyStateStore](stove0-core-sqlalchemystatestore.md)

## Governing policies

- <a id="pa-1e54390e21"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.SqlAlchemyStateStore.load_target_settlement_seal`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 57bc69686db03b08f7013d5ce5ecb797f503b72672d0422ef2bbe528bbd483a8 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'TargetSettlementSealRecord | None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "load_target_settlement_seal",
  "owner": "stove0_core.SqlAlchemyStateStore",
  "unit": "member"
}
```
