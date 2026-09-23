# review0_target_contracts.ReviewSamplePlan.canonical_windows

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-contracts:review0-target-contracts-reviewsampleplan-326de0378e:4afd46a94d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d2e8567eeb"></a>
- <a id="s-da7d977071"></a>`distribution`: `review0-target-contracts`
- <a id="s-b6a03bdb46"></a>`module`: `review0_target_contracts`
- <a id="s-67e49cf864"></a>`name`: `canonical_windows`
- <a id="s-7f483bad21"></a>`owner`: `review0_target_contracts.ReviewSamplePlan`
- <a id="s-7076903515"></a>`unit`: `member`

### Declared structure

- <a id="s-2e8aaeb51a"></a>`kind`: `"classmethod"`
- <a id="s-a2db1c73e2"></a>`signature`: `"\"(cls, value: 'tuple[ReviewSampleWindow, ...]') -> 'tuple[ReviewSampleWindow, ...]'\""`

## Maintained corroboration

### Related interface records

- [ReviewSamplePlan](review0-target-contracts-reviewsampleplan.md)

## Governing policies

- <a id="pa-3a11f12e17"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-contracts:review0_target_contracts](../../../evidence/sources/authorities.md#src-5615f251e5) — [some-implementations/stove0/review0/contracts/src/review0\_target\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/contracts/src/review0_target_contracts/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_contracts.ReviewSamplePlan.canonical_windows`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0426ef962dd6595f9f6ccef4dd6f05579f5966aafbd4cbef7b391a1566068855 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[ReviewSampleWindow, ...]') -> 'tuple[ReviewSampleWindow, ...]'\""
  },
  "distribution": "review0-target-contracts",
  "module": "review0_target_contracts",
  "name": "canonical_windows",
  "owner": "review0_target_contracts.ReviewSamplePlan",
  "unit": "member"
}
```

</details>
