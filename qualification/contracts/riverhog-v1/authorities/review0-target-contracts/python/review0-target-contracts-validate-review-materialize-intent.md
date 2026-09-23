# review0_target_contracts.validate_review_materialize_intent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-contracts:review0-target-contracts-validate-review-143e43ea44:ecce1ced12 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6e0ffd1e82"></a>
- <a id="s-8485acb51e"></a>`distribution`: `review0-target-contracts`
- <a id="s-4ec42e82fc"></a>`module`: `review0_target_contracts`
- <a id="s-2e3e679f25"></a>`name`: `validate_review_materialize_intent`
- <a id="s-c390f9418e"></a>`unit`: `export`

### Declared structure

- <a id="s-6ff21d095b"></a>`kind`: `"function"`
- <a id="s-5ff14b35ae"></a>`signature`: `"\"(intent: 'Mapping[str, object]') -> 'None'\""`

## Governing policies

- <a id="pa-3bc268f10a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-contracts:review0_target_contracts](../../../evidence/sources/authorities.md#src-5615f251e5) — [some-implementations/stove0/review0/contracts/src/review0\_target\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/contracts/src/review0_target_contracts/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_contracts.validate_review_materialize_intent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1c2a1bc60373ba0bb6cc007cb7c8a46954d6d09cd2962e492a5793cf9101b44c -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(intent: 'Mapping[str, object]') -> 'None'\""
  },
  "distribution": "review0-target-contracts",
  "module": "review0_target_contracts",
  "name": "validate_review_materialize_intent",
  "unit": "export"
}
```

</details>
