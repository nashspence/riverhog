# stove0_media_sampling_observer_contracts.validate_media_sampling_facts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-sampling-observer-contracts:stove0-media-sampling-observer-contracts-2a81764303:1040f86cb7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-sampling-observer-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d72565c267"></a>
- <a id="s-ace1134905"></a>`distribution`: `stove0-media-sampling-observer-contracts`
- <a id="s-ee2d09c295"></a>`module`: `stove0_media_sampling_observer_contracts`
- <a id="s-194d061467"></a>`name`: `validate_media_sampling_facts`
- <a id="s-b8c28b9b59"></a>`unit`: `export`

### Declared structure

- <a id="s-b8670c51c5"></a>`kind`: `"function"`
- <a id="s-fa3a73de36"></a>`signature`: `"\"(facts: 'Mapping[str, object]', subjects: 'Sequence[ArtifactSubject]') -> 'MediaSamplingFacts'\""`

## Governing policies

- <a id="pa-c66b40b959"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-media-sampling-observer-contracts:stove0_media_sampling_observer_contracts](../../../evidence/sources/authorities.md#src-b9344d06d7) — [some-implementations/stove0/observers/contracts/media-sampling/src/stove0\_media\_sampling\_observer\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/observers/contracts/media-sampling/src/stove0_media_sampling_observer_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_media_sampling_observer_contracts.validate_media_sampling_facts`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6f16ec78cb3dfb48197335b423013e21295a01c172a7b4e0d516ee582facc302 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(facts: 'Mapping[str, object]', subjects: 'Sequence[ArtifactSubject]') -> 'MediaSamplingFacts'\""
  },
  "distribution": "stove0-media-sampling-observer-contracts",
  "module": "stove0_media_sampling_observer_contracts",
  "name": "validate_media_sampling_facts",
  "unit": "export"
}
```

</details>
