# stove0_observer_client.load_semantic_validator_registry

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-client:stove0-observer-client-load-semantic-vali-5953f336f0:77992791ac -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6b5111100c"></a>
- <a id="s-a72b18683f"></a>`distribution`: `stove0-observer-client`
- <a id="s-80700fd125"></a>`module`: `stove0_observer_client`
- <a id="s-651e8e9586"></a>`name`: `load_semantic_validator_registry`
- <a id="s-475f3129ce"></a>`unit`: `export`

### Declared structure

- <a id="s-55c7f8470b"></a>`kind`: `"function"`
- <a id="s-24c12bfd9f"></a>`signature`: `"\"(provider_names: 'Sequence[str]') -> 'SemanticValidatorRegistry'\""`

## Governing policies

- <a id="pa-a9aa9d3232"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-client:stove0_observer_client](../../../evidence/sources/authorities.md#src-67dbe161ba) — [reference/stove0/packages/observer-client/src/stove0\_observer\_client/\_\_init\_\_.py](../../../../../../reference/stove0/packages/observer-client/src/stove0_observer_client/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_client.load_semantic_validator_registry`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 73ccee47c54c2532c5616e26d344f8d6ee90bb27b9f88d95cc26aac1386360d7 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(provider_names: 'Sequence[str]') -> 'SemanticValidatorRegistry'\""
  },
  "distribution": "stove0-observer-client",
  "module": "stove0_observer_client",
  "name": "load_semantic_validator_registry",
  "unit": "export"
}
```

</details>
