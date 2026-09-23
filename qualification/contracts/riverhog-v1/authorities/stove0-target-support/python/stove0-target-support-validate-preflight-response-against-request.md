# stove0_target_support.validate_preflight_response_against_request

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-validate-preflight-a19f367a9d:076f231e5e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-aa3f95cdf5"></a>
- <a id="s-654dd83060"></a>`distribution`: `stove0-target-support`
- <a id="s-5b9789292c"></a>`module`: `stove0_target_support`
- <a id="s-7608327ec7"></a>`name`: `validate_preflight_response_against_request`
- <a id="s-82a0673fe2"></a>`unit`: `export`

### Declared structure

- <a id="s-5d10ff2d40"></a>`kind`: `"function"`
- <a id="s-dd5cb93398"></a>`signature`: `"\"(response: 'TargetPreflightResponse', request: 'TargetPreflightRequest') -> 'None'\""`

## Governing policies

- <a id="pa-a97cecc86d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.validate_preflight_response_against_request`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bd9c47bbadc87e0a8656d4091385053e1d966745be82220296d71938a44442ed -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(response: 'TargetPreflightResponse', request: 'TargetPreflightRequest') -> 'None'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "validate_preflight_response_against_request",
  "unit": "export"
}
```

</details>
