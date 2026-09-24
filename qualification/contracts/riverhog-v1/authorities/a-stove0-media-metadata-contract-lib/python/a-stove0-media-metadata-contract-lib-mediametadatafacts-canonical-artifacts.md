# a_stove0_media_metadata_contract_lib.MediaMetadataFacts.canonical_artifacts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-metadata-contract-lib:a-stove0-media-metadata-contract-lib-medi-8c8dd5feda:cbcbfe3334 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-metadata-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8899c6f683"></a>
- <a id="s-8051d7ba41"></a>`distribution`: `a-stove0-media-metadata-contract-lib`
- <a id="s-3e2ac30622"></a>`module`: `a_stove0_media_metadata_contract_lib`
- <a id="s-3831618b58"></a>`name`: `canonical_artifacts`
- <a id="s-dc640f8613"></a>`owner`: `a_stove0_media_metadata_contract_lib.MediaMetadataFacts`
- <a id="s-40e677f0f2"></a>`unit`: `member`

### Declared structure

- <a id="s-ee2ebf1233"></a>`kind`: `"classmethod"`
- <a id="s-4f76bb0d62"></a>`signature`: `"\"(cls, value: 'tuple[MediaArtifactFacts, ...]') -> 'tuple[MediaArtifactFacts, ...]'\""`

## Maintained corroboration

### Related interface records

- [MediaMetadataFacts](a-stove0-media-metadata-contract-lib-mediametadatafacts.md)

## Governing policies

- <a id="pa-8165a12250"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-metadata-contract-lib:a_stove0_media_metadata_contract_lib](../../../evidence/sources/authorities.md#src-9ad9aed69d) — [some-implementations/stove0/observers/contracts/media-metadata/src/a\_stove0\_media\_metadata\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/observers/contracts/media-metadata/src/a_stove0_media_metadata_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_metadata_contract_lib.MediaMetadataFacts.canonical_artifacts`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8611c1402bf5c49ce37a4d0a86824d3819cbdd15d3cf778d50be4f8a2dd205a1 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[MediaArtifactFacts, ...]') -> 'tuple[MediaArtifactFacts, ...]'\""
  },
  "distribution": "a-stove0-media-metadata-contract-lib",
  "module": "a_stove0_media_metadata_contract_lib",
  "name": "canonical_artifacts",
  "owner": "a_stove0_media_metadata_contract_lib.MediaMetadataFacts",
  "unit": "member"
}
```

</details>
