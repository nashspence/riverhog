# stove0_target_support.validate_declaration_against_operation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-validate-declaratio-7e5a6b7c70:1d3f2057b2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c9aca64edb"></a>
- <a id="s-098ff76899"></a>`distribution`: `stove0-target-support`
- <a id="s-b4909e825e"></a>`module`: `stove0_target_support`
- <a id="s-723eba9ced"></a>`name`: `validate_declaration_against_operation`
- <a id="s-19858a9fa0"></a>`unit`: `export`

### Declared structure

- <a id="s-23d5417b8e"></a>`kind`: `"function"`
- <a id="s-1492cf79a8"></a>`signature`: `"\"(declaration: 'TargetDeclaration', operation: 'OperationContract') -> 'None'\""`

## Governing policies

- <a id="pa-a848e15bf5"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [reference/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.validate_declaration_against_operation`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d131a1bab872fd6684bba71ce3c59170c32b7a68d527e05fa38a611108197296 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(declaration: 'TargetDeclaration', operation: 'OperationContract') -> 'None'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "validate_declaration_against_operation",
  "unit": "export"
}
```

</details>
