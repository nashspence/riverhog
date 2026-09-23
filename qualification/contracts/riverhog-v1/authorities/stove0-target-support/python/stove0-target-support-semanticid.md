# stove0_target_support.SemanticId

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-semanticid:f189dfa7a9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-20f85db096"></a>
- <a id="s-3e4edff4ea"></a>`distribution`: `stove0-target-support`
- <a id="s-06c40aee0f"></a>`module`: `stove0_target_support`
- <a id="s-555188739b"></a>`name`: `SemanticId`
- <a id="s-c4919a3f7d"></a>`unit`: `export`

### Declared structure

- <a id="s-0fa2e05a79"></a>`kind`: `"object"`
- <a id="s-c31801a11e"></a>`type`: `"typing._AnnotatedAlias"`

## Governing policies

- <a id="pa-6cb09e18ee"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.SemanticId`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5ee7a0b2a02cc7cc74cb5a2aa34d3e7df750f1f1a61edda801b737423c2eb106 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "typing._AnnotatedAlias"
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "SemanticId",
  "unit": "export"
}
```

</details>
