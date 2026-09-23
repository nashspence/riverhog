# review0_target_contracts.ReviewSamplePlanPayload.canonical_windows

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-contracts:review0-target-contracts-reviewsampleplan-08fd8b4f23:306300562b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-610c7cf01e"></a>
- <a id="s-b52437a542"></a>`distribution`: `review0-target-contracts`
- <a id="s-75d7a8e213"></a>`module`: `review0_target_contracts`
- <a id="s-65e4d00780"></a>`name`: `canonical_windows`
- <a id="s-ff047fa4e3"></a>`owner`: `review0_target_contracts.ReviewSamplePlanPayload`
- <a id="s-f15f0c2096"></a>`unit`: `member`

### Declared structure

- <a id="s-deb650be4b"></a>`kind`: `"classmethod"`
- <a id="s-f20f0e7fdc"></a>`signature`: `"\"(cls, value: 'tuple[ReviewSampleWindow, ...]') -> 'tuple[ReviewSampleWindow, ...]'\""`

## Maintained corroboration

### Related interface records

- [ReviewSamplePlanPayload](review0-target-contracts-reviewsampleplanpayload.md)

## Governing policies

- <a id="pa-36c575d3c1"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-contracts:review0_target_contracts](../../../evidence/sources/authorities.md#src-5615f251e5) — [some-implementations/stove0/review0/contracts/src/review0\_target\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/contracts/src/review0_target_contracts/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_contracts.ReviewSamplePlanPayload.canonical_windows`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 252f872a6c8a1140759c854c855a3ccda549c5612d499d4a86c6deea05188317 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[ReviewSampleWindow, ...]') -> 'tuple[ReviewSampleWindow, ...]'\""
  },
  "distribution": "review0-target-contracts",
  "module": "review0_target_contracts",
  "name": "canonical_windows",
  "owner": "review0_target_contracts.ReviewSamplePlanPayload",
  "unit": "member"
}
```

</details>
