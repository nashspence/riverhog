# stove0_protocol.ARTIFACT_SELECTION_PAGE_MAX

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-artifact-selection-page-max:dbf12e553c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bf1010af6c"></a>
- <a id="s-2b7b466e71"></a>`distribution`: `stove0-protocol`
- <a id="s-19fa64d6d0"></a>`module`: `stove0_protocol`
- <a id="s-86ed19db01"></a>`name`: `ARTIFACT_SELECTION_PAGE_MAX`
- <a id="s-01187d2863"></a>`unit`: `export`

### Declared structure

- <a id="s-740e7e708e"></a>`kind`: `"constant"`
- <a id="s-9fa613a4f3"></a>`value`: `256`

## Governing policies

- <a id="pa-ad7032c1ec"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.ARTIFACT_SELECTION_PAGE_MAX`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 803a13c803b1202e7802c934fb78d49948c6551714627da1690acf6b71b887eb -->

```json
{
  "contract": {
    "kind": "constant",
    "value": 256
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "ARTIFACT_SELECTION_PAGE_MAX",
  "unit": "export"
}
```

</details>
