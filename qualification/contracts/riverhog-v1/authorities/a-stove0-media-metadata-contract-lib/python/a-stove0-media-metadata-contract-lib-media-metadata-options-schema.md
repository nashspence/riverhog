# a_stove0_media_metadata_contract_lib.MEDIA_METADATA_OPTIONS_SCHEMA

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-metadata-contract-lib:a-stove0-media-metadata-contract-lib-medi-6de70038c9:47e120e37a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-metadata-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e7bc2d2c8a"></a>
- <a id="s-7d3ba06019"></a>`distribution`: `a-stove0-media-metadata-contract-lib`
- <a id="s-0880a51a22"></a>`module`: `a_stove0_media_metadata_contract_lib`
- <a id="s-71b39cc267"></a>`name`: `MEDIA_METADATA_OPTIONS_SCHEMA`
- <a id="s-9c14c60af4"></a>`unit`: `export`

### Declared structure

- <a id="s-b15d1d8324"></a>`kind`: `"object"`
- <a id="s-011718b5e8"></a>`type`: `"stove0_protocol.models.JsonSchemaValidationProfile"`

## Governing policies

- <a id="pa-214147a6ef"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-metadata-contract-lib:a_stove0_media_metadata_contract_lib](../../../evidence/sources/authorities.md#src-9ad9aed69d) — [some-implementations/stove0/observers/contracts/media-metadata/src/a\_stove0\_media\_metadata\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/observers/contracts/media-metadata/src/a_stove0_media_metadata_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_metadata_contract_lib.MEDIA_METADATA_OPTIONS_SCHEMA`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 683e57f513c454622c64ca45fcecf95cb070d48e9834d8b4638a01a9e43b0685 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "stove0_protocol.models.JsonSchemaValidationProfile"
  },
  "distribution": "a-stove0-media-metadata-contract-lib",
  "module": "a_stove0_media_metadata_contract_lib",
  "name": "MEDIA_METADATA_OPTIONS_SCHEMA",
  "unit": "export"
}
```

</details>
