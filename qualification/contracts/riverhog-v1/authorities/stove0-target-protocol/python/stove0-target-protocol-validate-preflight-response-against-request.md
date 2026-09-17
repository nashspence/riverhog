# stove0_target_protocol.validate_preflight_response_against_request

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-validate-preflight-77e158f534:7b1da06bd4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-de75470e31"></a>
- <a id="s-5515de0c31"></a>`distribution`: `stove0-target-protocol`
- <a id="s-9c59433596"></a>`module`: `stove0_target_protocol`
- <a id="s-b8870a4534"></a>`name`: `validate_preflight_response_against_request`
- <a id="s-dd7ef76415"></a>`unit`: `export`

### Declared structure

- <a id="s-03be6f7cad"></a>`kind`: `"function"`
- <a id="s-477722826f"></a>`signature`: `"\"(response: 'TargetPreflightResponse', request: 'TargetPreflightRequest') -> 'None'\""`

## Governing policies

- <a id="pa-6d77e2c8f3"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [reference/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.validate_preflight_response_against_request`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 68cf68c9d379c38d7c5b3f1099851c5a9b1f9384e54233ea099a2d0eb1ceacb3 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(response: 'TargetPreflightResponse', request: 'TargetPreflightRequest') -> 'None'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "validate_preflight_response_against_request",
  "unit": "export"
}
```

</details>
