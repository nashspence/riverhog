# stove0_media_archive_target_contracts.AUDIO_ARCHIVE_OPERATION

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-contracts:stove0-media-archive-target-contracts-aud-1d828d1e02:3ca5339d82 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-68171345f5"></a>
| Field | Shape |
|---|---|
| <a id="s-0d2f6b81e7"></a>`contract` | type="stove0_target_protocol.protocol.OperationContract"; additional keys=`kind` |
| <a id="s-0e9bb7ac18"></a>`distribution` | "stove0-media-archive-target-contracts" |
| <a id="s-ee21fe2e90"></a>`module` | "stove0_media_archive_target_contracts" |
| <a id="s-c6f1dbfb2b"></a>`name` | "AUDIO_ARCHIVE_OPERATION" |
| <a id="s-331a3278d7"></a>`unit` | "export" |

## Governing policies

- <a id="pa-130c2cb325"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-archive-target-contracts:stove0_media_archive_target_contracts](../../../evidence/sources.md#src-dfeb5229f2) — `reference/stove0/targets/media-archive/contracts/src/stove0_media_archive_target_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_archive_target_contracts.AUDIO_ARCHIVE_OPERATION`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cdc42eacc252159bd2301dc92e21c9529b7cf88056ca898a40ad5cd291bee80e -->

```json
{
  "contract": {
    "kind": "object",
    "type": "stove0_target_protocol.protocol.OperationContract"
  },
  "distribution": "stove0-media-archive-target-contracts",
  "module": "stove0_media_archive_target_contracts",
  "name": "AUDIO_ARCHIVE_OPERATION",
  "unit": "export"
}
```
