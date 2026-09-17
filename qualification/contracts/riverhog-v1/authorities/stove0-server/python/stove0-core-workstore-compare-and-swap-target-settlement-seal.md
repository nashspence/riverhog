# stove0_core.WorkStore.compare_and_swap_target_settlement_seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-workstore-compare-and-swap-ta-b5c1789954:fe2a1987ce -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8d43d2da1b"></a>
- <a id="s-8a072306d8"></a>`distribution`: `stove0-server`
- <a id="s-14ec58eb2a"></a>`module`: `stove0_core`
- <a id="s-967a6db09d"></a>`name`: `compare_and_swap_target_settlement_seal`
- <a id="s-187a85729a"></a>`owner`: `stove0_core.WorkStore`
- <a id="s-1a8cd03631"></a>`unit`: `member`

### Declared structure

- <a id="s-09abd7a2c5"></a>`kind`: `"method"`
- <a id="s-7be4e9d402"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str', *, expected_revision: 'int', replacement: 'TargetSettlementSealRecord') -> 'TargetSettlementSealRecord'\""`

## Maintained corroboration

### Related interface records

- [WorkStore](stove0-core-workstore.md)

## Governing policies

- <a id="pa-b81bc5fbca"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.WorkStore.compare_and_swap_target_settlement_seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f23575dcf758de589509a05061a219a6e610576b92eb8ca91297476f6ee03f2e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str', *, expected_revision: 'int', replacement: 'TargetSettlementSealRecord') -> 'TargetSettlementSealRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "compare_and_swap_target_settlement_seal",
  "owner": "stove0_core.WorkStore",
  "unit": "member"
}
```

</details>
