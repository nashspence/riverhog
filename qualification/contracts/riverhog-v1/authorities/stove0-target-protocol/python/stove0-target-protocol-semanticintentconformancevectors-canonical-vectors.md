# stove0_target_protocol.SemanticIntentConformanceVectors.canonical_vectors

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-semanticintentconf-459f8d26f8:d0e5e8275e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e7fae839b5"></a>
- <a id="s-478d80d9e8"></a>`distribution`: `stove0-target-protocol`
- <a id="s-024398a09c"></a>`module`: `stove0_target_protocol`
- <a id="s-18f8f7d083"></a>`name`: `canonical_vectors`
- <a id="s-2cd8a8065c"></a>`owner`: `stove0_target_protocol.SemanticIntentConformanceVectors`
- <a id="s-04934c0064"></a>`unit`: `member`

### Declared structure

- <a id="s-6408ab3fef"></a>`kind`: `"classmethod"`
- <a id="s-da280568e1"></a>`signature`: `"\"(cls, value: 'tuple[SemanticIntentConformanceVector, ...]') -> 'tuple[SemanticIntentConformanceVector, ...]'\""`

## Maintained corroboration

### Related interface records

- [SemanticIntentConformanceVectors](stove0-target-protocol-semanticintentconformancevectors.md)

## Governing policies

- <a id="pa-73065234e9"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [reference/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.SemanticIntentConformanceVectors.canonical_vectors`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b8470e4cff8c8fbe415e18e19d174e6754cf8ecabbfbeb2ca742a3d7e319a878 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[SemanticIntentConformanceVector, ...]') -> 'tuple[SemanticIntentConformanceVector, ...]'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "canonical_vectors",
  "owner": "stove0_target_protocol.SemanticIntentConformanceVectors",
  "unit": "member"
}
```

</details>
