# a_stove0_media_metadata_contract_lib.MediaArtifactFacts.valid_state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-metadata-contract-lib:a-stove0-media-metadata-contract-lib-medi-fa48e07849:4b8486e9f4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-metadata-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a4c6f5bce9"></a>
- <a id="s-53b4db6086"></a>`distribution`: `a-stove0-media-metadata-contract-lib`
- <a id="s-09c8e327fa"></a>`module`: `a_stove0_media_metadata_contract_lib`
- <a id="s-fc94280b5b"></a>`name`: `valid_state`
- <a id="s-03dcfd8988"></a>`owner`: `a_stove0_media_metadata_contract_lib.MediaArtifactFacts`
- <a id="s-0e1afae4a2"></a>`unit`: `member`

### Declared structure

- <a id="s-806f2519f4"></a>`kind`: `"method"`
- <a id="s-f23151b175"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [MediaArtifactFacts](a-stove0-media-metadata-contract-lib-mediaartifactfacts.md)

## Governing policies

- <a id="pa-19ede64159"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-metadata-contract-lib:a_stove0_media_metadata_contract_lib](../../../evidence/sources/authorities.md#src-9ad9aed69d) — [some-implementations/stove0/observers/contracts/media-metadata/src/a\_stove0\_media\_metadata\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/observers/contracts/media-metadata/src/a_stove0_media_metadata_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_metadata_contract_lib.MediaArtifactFacts.valid_state`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 49d757634874b27ad2cd1e1b7af221ee99b0fdd36d7eb54fe19ba967475719a6 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "a-stove0-media-metadata-contract-lib",
  "module": "a_stove0_media_metadata_contract_lib",
  "name": "valid_state",
  "owner": "a_stove0_media_metadata_contract_lib.MediaArtifactFacts",
  "unit": "member"
}
```

</details>
