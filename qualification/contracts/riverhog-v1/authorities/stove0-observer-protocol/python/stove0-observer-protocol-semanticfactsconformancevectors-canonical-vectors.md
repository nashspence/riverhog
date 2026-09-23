# stove0_observer_protocol.SemanticFactsConformanceVectors.canonical_vectors

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-semanticfactscon-9c421519d6:a7cde585f5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-45850c74e4"></a>
- <a id="s-3d27a301c2"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-a84118383f"></a>`module`: `stove0_observer_protocol`
- <a id="s-78dd4fa8e6"></a>`name`: `canonical_vectors`
- <a id="s-e86ff32f12"></a>`owner`: `stove0_observer_protocol.SemanticFactsConformanceVectors`
- <a id="s-42beae02c0"></a>`unit`: `member`

### Declared structure

- <a id="s-5685e445cb"></a>`kind`: `"classmethod"`
- <a id="s-264ac9d58f"></a>`signature`: `"\"(cls, value: 'tuple[SemanticFactsConformanceVector, ...]') -> 'tuple[SemanticFactsConformanceVector, ...]'\""`

## Maintained corroboration

### Related interface records

- [SemanticFactsConformanceVectors](stove0-observer-protocol-semanticfactsconformancevectors.md)

## Governing policies

- <a id="pa-1d1a60ec69"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [some-implementations/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.SemanticFactsConformanceVectors.canonical_vectors`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7f788bb068056c987ea009b00d458509b8f86a976342e215ad5716d265342208 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[SemanticFactsConformanceVector, ...]') -> 'tuple[SemanticFactsConformanceVector, ...]'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "canonical_vectors",
  "owner": "stove0_observer_protocol.SemanticFactsConformanceVectors",
  "unit": "member"
}
```

</details>
