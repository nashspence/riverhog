# stove0_core.WorkStore.compare_and_swap_target_production_seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-workstore-compare-and-swap-ta-874d834140:76f543d691 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6d78499e63"></a>
- <a id="s-8ebbd08629"></a>`distribution`: `stove0-server`
- <a id="s-20bb8e29e2"></a>`module`: `stove0_core`
- <a id="s-ee6a669801"></a>`name`: `compare_and_swap_target_production_seal`
- <a id="s-49aef95453"></a>`owner`: `stove0_core.WorkStore`
- <a id="s-dc37908fc2"></a>`unit`: `member`

### Declared structure

- <a id="s-19f3937023"></a>`kind`: `"method"`
- <a id="s-0c817b3f38"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str', *, expected_revision: 'int', replacement: 'TargetProductionSealRecord') -> 'TargetProductionSealRecord'\""`

## Maintained corroboration

### Related interface records

- [WorkStore](stove0-core-workstore.md)

## Governing policies

- <a id="pa-943166292e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.WorkStore.compare_and_swap_target_production_seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f14b119c048d6c12aa4e35a4b9e3dd9347195473dd9603fce46e6973bea2105f -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str', *, expected_revision: 'int', replacement: 'TargetProductionSealRecord') -> 'TargetProductionSealRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "compare_and_swap_target_production_seal",
  "owner": "stove0_core.WorkStore",
  "unit": "member"
}
```

</details>
