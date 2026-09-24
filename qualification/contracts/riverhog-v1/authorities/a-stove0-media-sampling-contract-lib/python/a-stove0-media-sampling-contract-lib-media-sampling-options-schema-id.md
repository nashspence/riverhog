# a_stove0_media_sampling_contract_lib.MEDIA_SAMPLING_OPTIONS_SCHEMA_ID

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-sampling-contract-lib:a-stove0-media-sampling-contract-lib-medi-b0c97998f9:dee885690c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-sampling-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e96f1e91e8"></a>
- <a id="s-adb0aa4de3"></a>`distribution`: `a-stove0-media-sampling-contract-lib`
- <a id="s-bc6b27b480"></a>`module`: `a_stove0_media_sampling_contract_lib`
- <a id="s-0d843c6f94"></a>`name`: `MEDIA_SAMPLING_OPTIONS_SCHEMA_ID`
- <a id="s-63138bb3a3"></a>`unit`: `export`

### Declared structure

- <a id="s-66b51d2156"></a>`kind`: `"constant"`
- <a id="s-e823bfb362"></a>`value`: `"stove0.review.media-sampling-options/v1"`

## Governing policies

- <a id="pa-adb0faa22c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-sampling-contract-lib:a_stove0_media_sampling_contract_lib](../../../evidence/sources/authorities.md#src-68256911b9) — [some-implementations/stove0/observers/contracts/media-sampling/src/a\_stove0\_media\_sampling\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/observers/contracts/media-sampling/src/a_stove0_media_sampling_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_sampling_contract_lib.MEDIA_SAMPLING_OPTIONS_SCHEMA_ID`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3166ef2fb5a10cd014fce55f872e35d802e1f4ef051d3c1155f56b2ea5173037 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0.review.media-sampling-options/v1"
  },
  "distribution": "a-stove0-media-sampling-contract-lib",
  "module": "a_stove0_media_sampling_contract_lib",
  "name": "MEDIA_SAMPLING_OPTIONS_SCHEMA_ID",
  "unit": "export"
}
```

</details>
