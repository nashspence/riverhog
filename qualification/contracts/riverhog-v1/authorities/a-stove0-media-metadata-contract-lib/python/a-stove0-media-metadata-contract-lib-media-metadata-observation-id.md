# a_stove0_media_metadata_contract_lib.MEDIA_METADATA_OBSERVATION_ID

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-metadata-contract-lib:a-stove0-media-metadata-contract-lib-medi-7d50a3c69c:7cc8b14586 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-metadata-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c0ba013c1f"></a>
- <a id="s-f47652fff5"></a>`distribution`: `a-stove0-media-metadata-contract-lib`
- <a id="s-bebec2b110"></a>`module`: `a_stove0_media_metadata_contract_lib`
- <a id="s-ac545788f6"></a>`name`: `MEDIA_METADATA_OBSERVATION_ID`
- <a id="s-f8fdb699e3"></a>`unit`: `export`

### Declared structure

- <a id="s-085ef172b9"></a>`kind`: `"constant"`
- <a id="s-b2d0632ac3"></a>`value`: `"stove0.media.metadata/v1"`

## Governing policies

- <a id="pa-4d42180379"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-metadata-contract-lib:a_stove0_media_metadata_contract_lib](../../../evidence/sources/authorities.md#src-9ad9aed69d) — [some-implementations/stove0/observers/contracts/media-metadata/src/a\_stove0\_media\_metadata\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/observers/contracts/media-metadata/src/a_stove0_media_metadata_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_metadata_contract_lib.MEDIA_METADATA_OBSERVATION_ID`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 69e348c2ef82de63502db3951ef18e2eb5feb6def7f174ce34e7505cfd8ce165 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0.media.metadata/v1"
  },
  "distribution": "a-stove0-media-metadata-contract-lib",
  "module": "a_stove0_media_metadata_contract_lib",
  "name": "MEDIA_METADATA_OBSERVATION_ID",
  "unit": "export"
}
```

</details>
