# stove0_protocol.resolve_selection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-resolve-selection:34f6195ac7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9ece95c9a4"></a>
- <a id="s-1bc32a0985"></a>`distribution`: `stove0-protocol`
- <a id="s-097c63cb0f"></a>`module`: `stove0_protocol`
- <a id="s-629f872fe9"></a>`name`: `resolve_selection`
- <a id="s-d04189cfde"></a>`unit`: `export`

### Declared structure

- <a id="s-20812cfae4"></a>`kind`: `"function"`
- <a id="s-3df2ee1777"></a>`signature`: `"\"(reference: 'ArtifactSelectionRef', selections: 'SelectionDocuments') -> 'ArtifactSelection'\""`

## Governing policies

- <a id="pa-569af3bfdc"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.resolve_selection`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 282807169809baa403a5e07f9a7ee66cab39d99a038eed3b49dbaf8840f27743 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(reference: 'ArtifactSelectionRef', selections: 'SelectionDocuments') -> 'ArtifactSelection'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "resolve_selection",
  "unit": "export"
}
```

</details>
