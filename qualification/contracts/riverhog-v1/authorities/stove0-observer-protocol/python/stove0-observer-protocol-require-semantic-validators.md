# stove0_observer_protocol.require_semantic_validators

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-require-semantic-validators:c8997b23bb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-760480cf63"></a>
- <a id="s-5387e922a9"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-4061dc3b45"></a>`module`: `stove0_observer_protocol`
- <a id="s-f36d781bb4"></a>`name`: `require_semantic_validators`
- <a id="s-d95d0e82b2"></a>`unit`: `export`

### Declared structure

- <a id="s-09aa97af47"></a>`kind`: `"function"`
- <a id="s-d6bad46eff"></a>`signature`: `"\"(provider: 'SemanticValidatorProvider \| None', descriptor: 'ObserverDescriptor') -> 'None'\""`

## Governing policies

- <a id="pa-2d75efc8af"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [some-implementations/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.require_semantic_validators`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cdcf0ca6a410774d6836bc5b79e937dd3b2b8bc780aeb463665e9c4b694f12d6 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(provider: 'SemanticValidatorProvider | None', descriptor: 'ObserverDescriptor') -> 'None'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "require_semantic_validators",
  "unit": "export"
}
```

</details>
