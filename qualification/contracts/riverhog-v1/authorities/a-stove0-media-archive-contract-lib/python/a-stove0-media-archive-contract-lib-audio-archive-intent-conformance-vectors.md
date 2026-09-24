# a_stove0_media_archive_contract_lib.AUDIO_ARCHIVE_INTENT_CONFORMANCE_VECTORS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-archive-contract-lib:a-stove0-media-archive-contract-lib-audio-5fd61d4663:8e55f1afff -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-archive-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-dfaa9abdf4"></a>
- <a id="s-19dbb1bda7"></a>`distribution`: `a-stove0-media-archive-contract-lib`
- <a id="s-a2518ad817"></a>`module`: `a_stove0_media_archive_contract_lib`
- <a id="s-15db273280"></a>`name`: `AUDIO_ARCHIVE_INTENT_CONFORMANCE_VECTORS`
- <a id="s-fc155ec613"></a>`unit`: `export`

### Declared structure

- <a id="s-0a18fc3faf"></a>`kind`: `"object"`
- <a id="s-a0f1bd09d8"></a>`type`: `"stove0_target_protocol.conformance.SemanticIntentConformanceVectors"`

## Governing policies

- <a id="pa-fb7d26a514"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-archive-contract-lib:a_stove0_media_archive_contract_lib](../../../evidence/sources/authorities.md#src-3f8c4376e0) — [some-implementations/stove0/targets/media-archive/contracts/src/a\_stove0\_media\_archive\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/contracts/src/a_stove0_media_archive_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_archive_contract_lib.AUDIO_ARCHIVE_INTENT_CONFORMANCE_VECTORS`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e6a6fa3b5ff471875f0abc009b1cd3f75015197951bb2d25d53d801708e52f6a -->

```json
{
  "contract": {
    "kind": "object",
    "type": "stove0_target_protocol.conformance.SemanticIntentConformanceVectors"
  },
  "distribution": "a-stove0-media-archive-contract-lib",
  "module": "a_stove0_media_archive_contract_lib",
  "name": "AUDIO_ARCHIVE_INTENT_CONFORMANCE_VECTORS",
  "unit": "export"
}
```

</details>
