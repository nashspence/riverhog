# stove0_core.SqlAlchemyStateStore.compare_and_swap_target_settlement_seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-sqlalchemystatestore-compare-e87878eeeb:6cbc126a1f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-119473ef60"></a>
- <a id="s-9f54f36ad9"></a>`distribution`: `stove0-server`
- <a id="s-72f21d0767"></a>`module`: `stove0_core`
- <a id="s-6beaeb43c9"></a>`name`: `compare_and_swap_target_settlement_seal`
- <a id="s-0cead23a52"></a>`owner`: `stove0_core.SqlAlchemyStateStore`
- <a id="s-cc88ff4bbd"></a>`unit`: `member`

### Declared structure

- <a id="s-cdebeca4e0"></a>`kind`: `"method"`
- <a id="s-7189c8f075"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str', *, expected_revision: 'int', replacement: 'TargetSettlementSealRecord') -> 'TargetSettlementSealRecord'\""`

## Maintained corroboration

### Related interface records

- [SqlAlchemyStateStore](stove0-core-sqlalchemystatestore.md)

## Governing policies

- <a id="pa-94f550bf45"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.SqlAlchemyStateStore.compare_and_swap_target_settlement_seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6999cc93f6128ca5b2f94ebbef73d33995f182d6b0571debc2f0217e7d2b1b60 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str', *, expected_revision: 'int', replacement: 'TargetSettlementSealRecord') -> 'TargetSettlementSealRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "compare_and_swap_target_settlement_seal",
  "owner": "stove0_core.SqlAlchemyStateStore",
  "unit": "member"
}
```

</details>
