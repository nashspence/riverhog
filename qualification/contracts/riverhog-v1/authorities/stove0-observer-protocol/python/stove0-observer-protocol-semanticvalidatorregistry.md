# stove0_observer_protocol.SemanticValidatorRegistry

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-semanticvalidatorregistry:0263a2d2d9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d18fc077f0"></a>
- <a id="s-48f3af9436"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-d7c8a7f518"></a>`module`: `stove0_observer_protocol`
- <a id="s-cfe65c8332"></a>`name`: `SemanticValidatorRegistry`
- <a id="s-feadc64429"></a>`unit`: `export`

### Declared structure

- <a id="s-137c1c4f15"></a>`kind`: `"class"`
- <a id="s-877fcf40af"></a>`signature`: `"\"(bindings: 'Iterable[SemanticValidatorBinding]' = ()) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [resolve](stove0-observer-protocol-semanticvalidatorregistry-resolve.md)

## Governing policies

- <a id="pa-20a598451f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [reference/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.SemanticValidatorRegistry`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2f9a342f6ee2489bb8a99f2f31c24b239449bdf44b2c0aede09e4826d1c3481b -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(bindings: 'Iterable[SemanticValidatorBinding]' = ()) -> 'None'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "SemanticValidatorRegistry",
  "unit": "export"
}
```

</details>
