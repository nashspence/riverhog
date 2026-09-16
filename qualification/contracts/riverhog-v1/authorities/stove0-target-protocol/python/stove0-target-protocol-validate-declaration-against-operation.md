# stove0_target_protocol.validate_declaration_against_operation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-validate-declarati-a5c64c975e:896397e865 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-000946ca49"></a>
- <a id="s-9f3fdf555f"></a>`distribution`: `stove0-target-protocol`
- <a id="s-35b1ffb756"></a>`module`: `stove0_target_protocol`
- <a id="s-0154eb91ea"></a>`name`: `validate_declaration_against_operation`
- <a id="s-281a96e339"></a>`unit`: `export`

### Declared structure

- <a id="s-ed5a9f2562"></a>`kind`: `"function"`
- <a id="s-340f128908"></a>`signature`: `"\"(declaration: 'TargetDeclaration', operation: 'OperationContract') -> 'None'\""`

## Governing policies

- <a id="pa-d0ffdd0550"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.validate_declaration_against_operation`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 71fbd858369c8824906116ea91a5642ab002464f6c7c968348b33e4fb6ec9e60 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(declaration: 'TargetDeclaration', operation: 'OperationContract') -> 'None'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "validate_declaration_against_operation",
  "unit": "export"
}
```

</details>
