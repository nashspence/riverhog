# a_stove0_media_sampling_contract_lib.MediaSamplingFacts.canonical_artifacts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-sampling-contract-lib:a-stove0-media-sampling-contract-lib-medi-4f0b244577:3809f76ce4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-sampling-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-94903b71e4"></a>
- <a id="s-52a117d6f1"></a>`distribution`: `a-stove0-media-sampling-contract-lib`
- <a id="s-e752eb43f3"></a>`module`: `a_stove0_media_sampling_contract_lib`
- <a id="s-a66cee34cd"></a>`name`: `canonical_artifacts`
- <a id="s-39cdbed9f1"></a>`owner`: `a_stove0_media_sampling_contract_lib.MediaSamplingFacts`
- <a id="s-f99f53fa11"></a>`unit`: `member`

### Declared structure

- <a id="s-6010b0fede"></a>`kind`: `"classmethod"`
- <a id="s-af80525e6b"></a>`signature`: `"\"(cls, value: 'tuple[MediaSamplingArtifactFacts, ...]') -> 'tuple[MediaSamplingArtifactFacts, ...]'\""`

## Maintained corroboration

### Related interface records

- [MediaSamplingFacts](a-stove0-media-sampling-contract-lib-mediasamplingfacts.md)

## Governing policies

- <a id="pa-0d721e8d23"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-sampling-contract-lib:a_stove0_media_sampling_contract_lib](../../../evidence/sources/authorities.md#src-68256911b9) — [some-implementations/stove0/observers/contracts/media-sampling/src/a\_stove0\_media\_sampling\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/observers/contracts/media-sampling/src/a_stove0_media_sampling_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_sampling_contract_lib.MediaSamplingFacts.canonical_artifacts`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 952836ab326f213e04320b19ce8fb85bda1352b5cffe3cf728663665944107b7 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[MediaSamplingArtifactFacts, ...]') -> 'tuple[MediaSamplingArtifactFacts, ...]'\""
  },
  "distribution": "a-stove0-media-sampling-contract-lib",
  "module": "a_stove0_media_sampling_contract_lib",
  "name": "canonical_artifacts",
  "owner": "a_stove0_media_sampling_contract_lib.MediaSamplingFacts",
  "unit": "member"
}
```

</details>
