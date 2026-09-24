# a_stove0_media_sampling_contract_lib.MEDIA_SAMPLING_OBSERVATION_ID

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-sampling-contract-lib:a-stove0-media-sampling-contract-lib-medi-67ba769df8:2ee60dd814 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-sampling-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f26eb0c855"></a>
- <a id="s-df146798da"></a>`distribution`: `a-stove0-media-sampling-contract-lib`
- <a id="s-fc81cad811"></a>`module`: `a_stove0_media_sampling_contract_lib`
- <a id="s-21629691ec"></a>`name`: `MEDIA_SAMPLING_OBSERVATION_ID`
- <a id="s-d273c99c9b"></a>`unit`: `export`

### Declared structure

- <a id="s-f1c40b0f04"></a>`kind`: `"constant"`
- <a id="s-b288199717"></a>`value`: `"stove0.review.media-sampling/v1"`

## Governing policies

- <a id="pa-76db1ce7d6"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-sampling-contract-lib:a_stove0_media_sampling_contract_lib](../../../evidence/sources/authorities.md#src-68256911b9) — [some-implementations/stove0/observers/contracts/media-sampling/src/a\_stove0\_media\_sampling\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/observers/contracts/media-sampling/src/a_stove0_media_sampling_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_sampling_contract_lib.MEDIA_SAMPLING_OBSERVATION_ID`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4a059fbaf6973e9c1ab6b94cb26182c564672c50d6e73211a01bd3da591798dd -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0.review.media-sampling/v1"
  },
  "distribution": "a-stove0-media-sampling-contract-lib",
  "module": "a_stove0_media_sampling_contract_lib",
  "name": "MEDIA_SAMPLING_OBSERVATION_ID",
  "unit": "export"
}
```

</details>
