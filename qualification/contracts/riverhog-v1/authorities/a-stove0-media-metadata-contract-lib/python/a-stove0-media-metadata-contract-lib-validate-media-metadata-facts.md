# a_stove0_media_metadata_contract_lib.validate_media_metadata_facts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-metadata-contract-lib:a-stove0-media-metadata-contract-lib-vali-f312459584:407e1ffeeb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-metadata-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-790fd0f482"></a>
- <a id="s-da2b7db8dc"></a>`distribution`: `a-stove0-media-metadata-contract-lib`
- <a id="s-3b30c6a14a"></a>`module`: `a_stove0_media_metadata_contract_lib`
- <a id="s-c59d24c542"></a>`name`: `validate_media_metadata_facts`
- <a id="s-15ab90e9c8"></a>`unit`: `export`

### Declared structure

- <a id="s-ff7ac06ab0"></a>`kind`: `"function"`
- <a id="s-4b95003789"></a>`signature`: `"\"(facts: 'Mapping[str, object]', subjects: 'Sequence[ArtifactSubject]') -> 'MediaMetadataFacts'\""`

## Governing policies

- <a id="pa-2bdadcc53c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-metadata-contract-lib:a_stove0_media_metadata_contract_lib](../../../evidence/sources/authorities.md#src-9ad9aed69d) — [some-implementations/stove0/observers/contracts/media-metadata/src/a\_stove0\_media\_metadata\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/observers/contracts/media-metadata/src/a_stove0_media_metadata_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_metadata_contract_lib.validate_media_metadata_facts`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7b88d82de2d3718680231ca61b2945b17f193dcf0f81d0ddc309d011f6526da6 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(facts: 'Mapping[str, object]', subjects: 'Sequence[ArtifactSubject]') -> 'MediaMetadataFacts'\""
  },
  "distribution": "a-stove0-media-metadata-contract-lib",
  "module": "a_stove0_media_metadata_contract_lib",
  "name": "validate_media_metadata_facts",
  "unit": "export"
}
```

</details>
