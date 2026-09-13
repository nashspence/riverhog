# stove0_media_archive_target_contracts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-contracts:stove0-media-archive-target-contracts:322688c53e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-34868960bf"></a>
| Field | Shape |
|---|---|
| <a id="s-e060c10763"></a>`candidate_id` | "python:stove0-media-archive-target-contracts:stove0_media_archive_target_contracts" |
| <a id="s-efc7beb30a"></a>`distribution` | "stove0-media-archive-target-contracts" |
| <a id="s-3358186e0c"></a>`exports` | additional keys=`AUDIO_ARCHIVE_INTENT_CONFORMANCE_VECTORS`, `AUDIO_ARCHIVE_INTENT_SEMANTICS`, `AUDIO_ARCHIVE_OPERATION`, `AUDIO_ARCHIVE_OPERATION_ID`, `AUDIO_ARCHIVE_ROLE`, `AV1_OPUS_ARCHIVE_INTENT_CONFORMANCE_VECTORS`, `AV1_OPUS_ARCHIVE_INTENT_SEMANTICS`, `AV1_OPUS_ARCHIVE_OPERATION`, `AV1_OPUS_ARCHIVE_OPERATION_ID`, `AV1_OPUS_ARCHIVE_ROLE`, `AudioArchiveIntent`, `Av1OpusArchiveIntent`, `METADATA_XMP_ROLE`, `MediaFieldPreference`, `MediaGps`, `MediaProjectionFieldName`, `MediaProjectionPolicy`, `OPERATIONS`, `SOURCE_ARTIFACT_ROLE`, `SOURCE_ROLE`, `XMP_SOURCE_ROLE`, `operation_contract`, `validate_audio_archive_intent`, `validate_av1_opus_archive_intent` |
| <a id="s-7522159a5d"></a>`module` | "stove0_media_archive_target_contracts" |

## Governing policies

- <a id="pa-c6df8e510e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-archive-target-contracts:stove0_media_archive_target_contracts](../../../evidence/sources.md#src-dfeb5229f2) — `reference/stove0/targets/media-archive/contracts/src/stove0_media_archive_target_contracts/__init__.py`

### Machine authority

- `/external_contract/python/35`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5151c9061f6351c0b31147409832c9c83bf0322dcdef65fce360a42d8af58da5 -->

```json
{
  "candidate_id": "python:stove0-media-archive-target-contracts:stove0_media_archive_target_contracts",
  "distribution": "stove0-media-archive-target-contracts",
  "exports": {
    "AUDIO_ARCHIVE_INTENT_CONFORMANCE_VECTORS": {
      "kind": "object",
      "type": "stove0_target_protocol.conformance.SemanticIntentConformanceVectors"
    },
    "AUDIO_ARCHIVE_INTENT_SEMANTICS": {
      "kind": "object",
      "type": "stove0_protocol.models.SemanticValidationProfile"
    },
    "AUDIO_ARCHIVE_OPERATION": {
      "kind": "object",
      "type": "stove0_target_protocol.protocol.OperationContract"
    },
    "AUDIO_ARCHIVE_OPERATION_ID": {
      "kind": "constant",
      "value": "stove0.media.audio-archive/v1"
    },
    "AUDIO_ARCHIVE_ROLE": {
      "kind": "constant",
      "value": "stove0.media.audio-archive/v1"
    },
    "AV1_OPUS_ARCHIVE_INTENT_CONFORMANCE_VECTORS": {
      "kind": "object",
      "type": "stove0_target_protocol.conformance.SemanticIntentConformanceVectors"
    },
    "AV1_OPUS_ARCHIVE_INTENT_SEMANTICS": {
      "kind": "object",
      "type": "stove0_protocol.models.SemanticValidationProfile"
    },
    "AV1_OPUS_ARCHIVE_OPERATION": {
      "kind": "object",
      "type": "stove0_target_protocol.protocol.OperationContract"
    },
    "AV1_OPUS_ARCHIVE_OPERATION_ID": {
      "kind": "constant",
      "value": "stove0.media.av1-opus-archive/v1"
    },
    "AV1_OPUS_ARCHIVE_ROLE": {
      "kind": "constant",
      "value": "stove0.media.av1-opus-archive/v1"
    },
    "AudioArchiveIntent": {
      "kind": "class",
      "schema_sha256": "ca8f0aa963958b27701a2b5a59429d23e242d2315c6d5361a9177c5a69d52456",
      "signature": "\"(*, codec: Literal['opus'] = 'opus', container: Literal['opus'] = 'opus', bitrate_kbps: Annotated[int, Ge(ge=16), Le(le=512)] = 128, metadata_projection: stove0_media_archive_target_contracts.projection_policy.MediaProjectionPolicy = <factory>) -> None\""
    },
    "Av1OpusArchiveIntent": {
      "kind": "class",
      "schema_sha256": "3f8fff646a6b9081f6724972b146e71d8f20995d9a96c6da30e53b9bcb18094e",
      "signature": "\"(*, codec: Literal['av1'] = 'av1', container: Literal['mkv'] = 'mkv', quality: Annotated[int, Ge(ge=0), Le(le=63)] = 23, max_height: Annotated[int | None, Ge(ge=144), Le(le=8640)] = None, audio_bitrate_kbps: Annotated[int, Ge(ge=16), Le(le=512)] = 128, salvage: Literal['off', 'safe-remux'] = 'safe-remux', metadata_projection: stove0_media_archive_target_contracts.projection_policy.MediaProjectionPolicy = <factory>) -> None\""
    },
    "METADATA_XMP_ROLE": {
      "kind": "constant",
      "value": "stove0.media.metadata-xmp/v1"
    },
    "MediaFieldPreference": {
      "kind": "class",
      "members": {
        "unique_fields": {
          "kind": "classmethod",
          "signature": "\"(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'\""
        }
      },
      "schema_sha256": "a116c8bee1c7caaea6c6f3063ea638d2000da8abd190574442ce7410d1e56e4d",
      "signature": "\"(*, name: Literal['capture-time', 'creator', 'device-make', 'device-model', 'gps-latitude', 'gps-longitude'], fields: Annotated[tuple[str, ...], MinLen(min_length=1)]) -> None\""
    },
    "MediaGps": {
      "kind": "class",
      "members": {
        "valid_position": {
          "kind": "method",
          "signature": "\"(self) -> 'Self'\""
        }
      },
      "schema_sha256": "034cbe1a80429c82cacda24116ace3547aae0d8cfe2dd1e814387d6ce49a8325",
      "signature": "'(*, latitude: float, longitude: float) -> None'"
    },
    "MediaProjectionFieldName": {
      "kind": "object",
      "type": "typing._LiteralGenericAlias"
    },
    "MediaProjectionPolicy": {
      "kind": "class",
      "members": {
        "canonical_creators": {
          "kind": "classmethod",
          "signature": "\"(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'\""
        },
        "canonical_optional_text": {
          "kind": "classmethod",
          "signature": "\"(cls, value: 'str | None') -> 'str | None'\""
        },
        "canonical_preferences": {
          "kind": "classmethod",
          "signature": "\"(cls, value: 'tuple[MediaFieldPreference, ...]') -> 'tuple[MediaFieldPreference, ...]'\""
        },
        "canonical_tags": {
          "kind": "classmethod",
          "signature": "\"(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'\""
        }
      },
      "schema_sha256": "77a33bbdb2a9e5f2d6191fab61273bc8f1eb5d357196a75ecf82c730a1dfd621",
      "signature": "\"(*, format: Literal['stove0-media-projection-policy/v1'] = 'stove0-media-projection-policy/v1', device_make: str | None = None, device_model: str | None = None, gps: stove0_media_archive_target_contracts.projection_policy.MediaGps | None = None, creators: tuple[str, ...] = (), tags: tuple[str, ...] = (), field_preferences: tuple[stove0_media_archive_target_contracts.projection_policy.MediaFieldPreference, ...] = ()) -> None\""
    },
    "OPERATIONS": {
      "kind": "object",
      "type": "builtins.dict"
    },
    "SOURCE_ARTIFACT_ROLE": {
      "kind": "constant",
      "value": "stove0.media.source-artifact/v1"
    },
    "SOURCE_ROLE": {
      "kind": "constant",
      "value": "stove0.media.source/v1"
    },
    "XMP_SOURCE_ROLE": {
      "kind": "constant",
      "value": "stove0.media.xmp-source/v1"
    },
    "operation_contract": {
      "kind": "function",
      "signature": "\"(operation_id: 'str') -> 'OperationContract'\""
    },
    "validate_audio_archive_intent": {
      "kind": "function",
      "signature": "\"(intent: 'Mapping[str, object]') -> 'None'\""
    },
    "validate_av1_opus_archive_intent": {
      "kind": "function",
      "signature": "\"(intent: 'Mapping[str, object]') -> 'None'\""
    }
  },
  "module": "stove0_media_archive_target_contracts"
}
```
