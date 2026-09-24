# a_stove0_media_sampling_contract_lib.validate_media_sampling_facts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-sampling-contract-lib:a-stove0-media-sampling-contract-lib-vali-89e6cf081b:958e99e277 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-sampling-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9024bfa1cc"></a>
- <a id="s-257fc393f4"></a>`distribution`: `a-stove0-media-sampling-contract-lib`
- <a id="s-a4c8309760"></a>`module`: `a_stove0_media_sampling_contract_lib`
- <a id="s-75464a8168"></a>`name`: `validate_media_sampling_facts`
- <a id="s-ef86e64de3"></a>`unit`: `export`

### Declared structure

- <a id="s-e9ed419155"></a>`kind`: `"function"`
- <a id="s-c33e50c5d2"></a>`signature`: `"\"(facts: 'Mapping[str, object]', subjects: 'Sequence[ArtifactSubject]') -> 'MediaSamplingFacts'\""`

## Governing policies

- <a id="pa-4b1486edfc"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-sampling-contract-lib:a_stove0_media_sampling_contract_lib](../../../evidence/sources/authorities.md#src-68256911b9) — [some-implementations/stove0/observers/contracts/media-sampling/src/a\_stove0\_media\_sampling\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/observers/contracts/media-sampling/src/a_stove0_media_sampling_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_sampling_contract_lib.validate_media_sampling_facts`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c39821ebb01a4253e0c27cdb1acd1ff8c8321c67c3328e0f522087c77958fc2d -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(facts: 'Mapping[str, object]', subjects: 'Sequence[ArtifactSubject]') -> 'MediaSamplingFacts'\""
  },
  "distribution": "a-stove0-media-sampling-contract-lib",
  "module": "a_stove0_media_sampling_contract_lib",
  "name": "validate_media_sampling_facts",
  "unit": "export"
}
```

</details>
