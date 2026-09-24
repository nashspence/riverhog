# a_stove0_media_sampling_contract_lib.MEDIA_SAMPLING_FACTS_SCHEMA_ID

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-sampling-contract-lib:a-stove0-media-sampling-contract-lib-medi-ea8bcbbf07:a454da9493 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-sampling-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9035d02f55"></a>
- <a id="s-a8ba5f5169"></a>`distribution`: `a-stove0-media-sampling-contract-lib`
- <a id="s-83dcbd02d4"></a>`module`: `a_stove0_media_sampling_contract_lib`
- <a id="s-b8ee1e35bf"></a>`name`: `MEDIA_SAMPLING_FACTS_SCHEMA_ID`
- <a id="s-2b77e321af"></a>`unit`: `export`

### Declared structure

- <a id="s-4c48db9689"></a>`kind`: `"constant"`
- <a id="s-f257cf3b93"></a>`value`: `"stove0.review.media-sampling-facts/v1"`

## Governing policies

- <a id="pa-89eec8a323"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-sampling-contract-lib:a_stove0_media_sampling_contract_lib](../../../evidence/sources/authorities.md#src-68256911b9) — [some-implementations/stove0/observers/contracts/media-sampling/src/a\_stove0\_media\_sampling\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/observers/contracts/media-sampling/src/a_stove0_media_sampling_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_sampling_contract_lib.MEDIA_SAMPLING_FACTS_SCHEMA_ID`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0a479ee03b564688ac09c1194859a4be9393e6c105cb8d6a2735d9b481c1435a -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0.review.media-sampling-facts/v1"
  },
  "distribution": "a-stove0-media-sampling-contract-lib",
  "module": "a_stove0_media_sampling_contract_lib",
  "name": "MEDIA_SAMPLING_FACTS_SCHEMA_ID",
  "unit": "export"
}
```

</details>
