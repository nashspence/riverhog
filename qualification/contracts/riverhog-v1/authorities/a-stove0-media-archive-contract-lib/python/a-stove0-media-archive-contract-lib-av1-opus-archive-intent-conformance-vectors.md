# a_stove0_media_archive_contract_lib.AV1_OPUS_ARCHIVE_INTENT_CONFORMANCE_VECTORS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-archive-contract-lib:a-stove0-media-archive-contract-lib-av1-o-6db894f0ff:be1af0c243 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-archive-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6179c4c5ce"></a>
- <a id="s-5ce36ebb50"></a>`distribution`: `a-stove0-media-archive-contract-lib`
- <a id="s-006c9fcf68"></a>`module`: `a_stove0_media_archive_contract_lib`
- <a id="s-d97fe2e125"></a>`name`: `AV1_OPUS_ARCHIVE_INTENT_CONFORMANCE_VECTORS`
- <a id="s-0d19b731a7"></a>`unit`: `export`

### Declared structure

- <a id="s-2dce6f9a55"></a>`kind`: `"object"`
- <a id="s-ce8dde99b5"></a>`type`: `"stove0_target_protocol.conformance.SemanticIntentConformanceVectors"`

## Governing policies

- <a id="pa-0fb2c1c615"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-archive-contract-lib:a_stove0_media_archive_contract_lib](../../../evidence/sources/authorities.md#src-3f8c4376e0) — [some-implementations/stove0/targets/media-archive/contracts/src/a\_stove0\_media\_archive\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/contracts/src/a_stove0_media_archive_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_archive_contract_lib.AV1_OPUS_ARCHIVE_INTENT_CONFORMANCE_VECTORS`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1e4fde3d8cf3ff178308bb3955bb74fd23158a43a3ef93d1f69dbca9500e4d48 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "stove0_target_protocol.conformance.SemanticIntentConformanceVectors"
  },
  "distribution": "a-stove0-media-archive-contract-lib",
  "module": "a_stove0_media_archive_contract_lib",
  "name": "AV1_OPUS_ARCHIVE_INTENT_CONFORMANCE_VECTORS",
  "unit": "export"
}
```

</details>
