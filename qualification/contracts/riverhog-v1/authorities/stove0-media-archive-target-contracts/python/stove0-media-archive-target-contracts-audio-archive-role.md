# stove0_media_archive_target_contracts.AUDIO_ARCHIVE_ROLE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-contracts:stove0-media-archive-target-contracts-aud-2301a4bea5:7ffc513971 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-65985aafb2"></a>
| Field | Shape |
|---|---|
| <a id="s-41a8c8abcc"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-19b1e69e4e"></a>`distribution` | "stove0-media-archive-target-contracts" |
| <a id="s-c6a515e82c"></a>`module` | "stove0_media_archive_target_contracts" |
| <a id="s-66d4088454"></a>`name` | "AUDIO_ARCHIVE_ROLE" |
| <a id="s-177325d20d"></a>`unit` | "export" |

## Governing policies

- <a id="pa-bff21777cb"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-archive-target-contracts:stove0_media_archive_target_contracts](../../../evidence/sources.md#src-dfeb5229f2) — `reference/stove0/targets/media-archive/contracts/src/stove0_media_archive_target_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_archive_target_contracts.AUDIO_ARCHIVE_ROLE`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ef4ba7359db014f1c3e385e8d8b0907a7c3903850d9136eea3b6c17a0a16f019 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0.media.audio-archive/v1"
  },
  "distribution": "stove0-media-archive-target-contracts",
  "module": "stove0_media_archive_target_contracts",
  "name": "AUDIO_ARCHIVE_ROLE",
  "unit": "export"
}
```
