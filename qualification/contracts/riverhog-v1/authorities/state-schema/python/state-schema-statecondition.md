# state_schema.StateCondition

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-statecondition:9397ac6661 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-afc85e35b6"></a>
- <a id="s-e0e6499c24"></a>`distribution`: `state-schema`
- <a id="s-d59ce41003"></a>`module`: `state_schema`
- <a id="s-9080cc8b1d"></a>`name`: `StateCondition`
- <a id="s-fdd4378040"></a>`unit`: `export`

### Declared structure

- <a id="s-166d44a9e0"></a>`kind`: `"object"`
- <a id="s-6a0e1068bf"></a>`type`: `"typing._LiteralGenericAlias"`

## Governing policies

- <a id="pa-a120fb07c0"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:state-schema:state_schema](../../../evidence/sources/authorities.md#src-57d87d192c) — [packages/state-schema/src/state\_schema/\_\_init\_\_.py](../../../../../../packages/state-schema/src/state_schema/__init__.py)

### Machine authority

- `/external_contract/python/state_schema.StateCondition`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0777894ba1b30a8212c0d637d1a6e3a055286f10c91e7afd4a37605798e44250 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "typing._LiteralGenericAlias"
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "StateCondition",
  "unit": "export"
}
```

</details>
