# a_stove0_media_metadata_contract_lib.MEDIA_METADATA_FACTS_SCHEMA_ID

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-metadata-contract-lib:a-stove0-media-metadata-contract-lib-medi-361e263e53:2ebe1457f0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-metadata-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6e58bfd53e"></a>
- <a id="s-c20a42f419"></a>`distribution`: `a-stove0-media-metadata-contract-lib`
- <a id="s-af20f5fc5f"></a>`module`: `a_stove0_media_metadata_contract_lib`
- <a id="s-960e8a55cf"></a>`name`: `MEDIA_METADATA_FACTS_SCHEMA_ID`
- <a id="s-7612f37e72"></a>`unit`: `export`

### Declared structure

- <a id="s-a22226d03c"></a>`kind`: `"constant"`
- <a id="s-12bc5220ad"></a>`value`: `"stove0.media.metadata-facts/v1"`

## Governing policies

- <a id="pa-9869030de3"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-metadata-contract-lib:a_stove0_media_metadata_contract_lib](../../../evidence/sources/authorities.md#src-9ad9aed69d) — [some-implementations/stove0/observers/contracts/media-metadata/src/a\_stove0\_media\_metadata\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/observers/contracts/media-metadata/src/a_stove0_media_metadata_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_metadata_contract_lib.MEDIA_METADATA_FACTS_SCHEMA_ID`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b16f39693871a9aa099712f236bd23cd4f3cf4a4a66109e68a4d092ac3c34ed8 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0.media.metadata-facts/v1"
  },
  "distribution": "a-stove0-media-metadata-contract-lib",
  "module": "a_stove0_media_metadata_contract_lib",
  "name": "MEDIA_METADATA_FACTS_SCHEMA_ID",
  "unit": "export"
}
```

</details>
