# a_stove0_media_metadata_contract_lib.MEDIA_METADATA_OPTIONS_SCHEMA_ID

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-metadata-contract-lib:a-stove0-media-metadata-contract-lib-medi-d8fbe5d196:2152abd93a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-metadata-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f2310e4202"></a>
- <a id="s-8d60f42776"></a>`distribution`: `a-stove0-media-metadata-contract-lib`
- <a id="s-7a85b160a0"></a>`module`: `a_stove0_media_metadata_contract_lib`
- <a id="s-7ddcf51c7b"></a>`name`: `MEDIA_METADATA_OPTIONS_SCHEMA_ID`
- <a id="s-699144bc9b"></a>`unit`: `export`

### Declared structure

- <a id="s-14d701cd62"></a>`kind`: `"constant"`
- <a id="s-eb94a09665"></a>`value`: `"stove0.media.metadata-options/v1"`

## Governing policies

- <a id="pa-bda0987474"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-metadata-contract-lib:a_stove0_media_metadata_contract_lib](../../../evidence/sources/authorities.md#src-9ad9aed69d) — [some-implementations/stove0/observers/contracts/media-metadata/src/a\_stove0\_media\_metadata\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/observers/contracts/media-metadata/src/a_stove0_media_metadata_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_metadata_contract_lib.MEDIA_METADATA_OPTIONS_SCHEMA_ID`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 13527f63c572ecf5c57b26c5b8140aa33b332dde8acd792906f9413c9dd2ec9e -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0.media.metadata-options/v1"
  },
  "distribution": "a-stove0-media-metadata-contract-lib",
  "module": "a_stove0_media_metadata_contract_lib",
  "name": "MEDIA_METADATA_OPTIONS_SCHEMA_ID",
  "unit": "export"
}
```

</details>
