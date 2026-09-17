# stove0_protocol.ARTIFACT_ID_PATTERN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-artifact-id-pattern:70cdbe86b8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b7640e1879"></a>
- <a id="s-0ecd64acbd"></a>`distribution`: `stove0-protocol`
- <a id="s-be067143a2"></a>`module`: `stove0_protocol`
- <a id="s-1e879f39af"></a>`name`: `ARTIFACT_ID_PATTERN`
- <a id="s-1de67ff77e"></a>`unit`: `export`

### Declared structure

- <a id="s-dd1b3ebead"></a>`kind`: `"constant"`
- <a id="s-caff340b51"></a>`value`: `"^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$"`

## Governing policies

- <a id="pa-4869aeefdf"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.ARTIFACT_ID_PATTERN`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 28e150595599788eba69f87c1aa7fb984fa651b527afecef799717e0684908e0 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$"
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "ARTIFACT_ID_PATTERN",
  "unit": "export"
}
```

</details>
