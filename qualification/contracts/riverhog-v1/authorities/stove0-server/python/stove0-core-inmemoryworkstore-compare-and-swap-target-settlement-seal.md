# stove0_core.InMemoryWorkStore.compare_and_swap_target_settlement_seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-inmemoryworkstore-compare-and-a5c44a945a:068c6d671e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f4dc5d4fbc"></a>
- <a id="s-4d2ee148a2"></a>`distribution`: `stove0-server`
- <a id="s-08a8fc68bd"></a>`module`: `stove0_core`
- <a id="s-d91f12cfb8"></a>`name`: `compare_and_swap_target_settlement_seal`
- <a id="s-6a57038474"></a>`owner`: `stove0_core.InMemoryWorkStore`
- <a id="s-2393e4bb66"></a>`unit`: `member`

### Declared structure

- <a id="s-6af8fb4000"></a>`kind`: `"method"`
- <a id="s-2db57f04cf"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str', *, expected_revision: 'int', replacement: 'TargetSettlementSealRecord') -> 'TargetSettlementSealRecord'\""`

## Maintained corroboration

### Related interface records

- [InMemoryWorkStore](stove0-core-inmemoryworkstore.md)

## Governing policies

- <a id="pa-db302ee95d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.InMemoryWorkStore.compare_and_swap_target_settlement_seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c72e5d90381df25a9defbafbd2605f943b57f7e52eb15a4f7b28546310adb41d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str', *, expected_revision: 'int', replacement: 'TargetSettlementSealRecord') -> 'TargetSettlementSealRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "compare_and_swap_target_settlement_seal",
  "owner": "stove0_core.InMemoryWorkStore",
  "unit": "member"
}
```

</details>
