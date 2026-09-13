# stove0_media_archive_target_support

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-support:stove0-media-archive-target-support:9cc4730537 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fea80d6fa4"></a>
| Field | Shape |
|---|---|
| <a id="s-b2163a49a2"></a>`candidate_id` | "python:stove0-media-archive-target-support:stove0_media_archive_target_support" |
| <a id="s-7fd3528204"></a>`distribution` | "stove0-media-archive-target-support" |
| <a id="s-8cc5b6cb64"></a>`exports` | additional keys=`MEDIA_FACT_PROJECTION_FIELDS`, `MEDIA_PROJECTION_FORMAT`, `MediaArchiveProjection`, `MediaArchiveProjectionPayload`, `MediaProjectedValue`, `MediaProjectionItem`, `RetainedXmpSidecar`, `ffmpeg_container_metadata_args`, `render_projection_xmp`, `resolve_media_archive_preflight_projection`, `resolve_media_archive_projection` |
| <a id="s-94c634c42b"></a>`module` | "stove0_media_archive_target_support" |

## Governing policies

- <a id="pa-a6149561b6"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-archive-target-support:stove0_media_archive_target_support](../../../evidence/sources.md#src-3caa343b1d) — `reference/stove0/targets/media-archive/support/src/stove0_media_archive_target_support/__init__.py`

### Machine authority

- `/external_contract/python/36`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5eefac2f618af93cf9340463e37f4ccb1e1bef7163e8642f3f58bb8055faaf20 -->

```json
{
  "candidate_id": "python:stove0-media-archive-target-support:stove0_media_archive_target_support",
  "distribution": "stove0-media-archive-target-support",
  "exports": {
    "MEDIA_FACT_PROJECTION_FIELDS": {
      "kind": "object",
      "type": "builtins.dict"
    },
    "MEDIA_PROJECTION_FORMAT": {
      "kind": "constant",
      "value": "stove0-media-archive-projection/v1"
    },
    "MediaArchiveProjection": {
      "kind": "class",
      "members": {
        "item_for": {
          "kind": "method",
          "signature": "\"(self, artifact_id: 'str') -> 'MediaProjectionItem'\""
        },
        "seal": {
          "kind": "classmethod",
          "signature": "\"(cls, payload: 'MediaArchiveProjectionPayload') -> 'MediaArchiveProjection'\""
        },
        "validate_plan_evidence": {
          "kind": "method",
          "signature": "\"(self, observation_result_sha256s: 'Sequence[str]') -> 'None'\""
        },
        "verify_digest": {
          "kind": "method",
          "signature": "\"(self) -> 'Self'\""
        }
      },
      "schema_sha256": "96d4a855d44b0272b52404a36f74d1fbee4ab33a060aa246a2fa2d070d7ccb8b",
      "signature": "\"(*, format: Literal['stove0-media-archive-projection/v1'] = 'stove0-media-archive-projection/v1', observation_result_sha256s: tuple[str, ...], items: Annotated[tuple[stove0_media_archive_target_support.projection.MediaProjectionItem, ...], MinLen(min_length=1)], retained_xmp_sidecars: tuple[stove0_media_archive_target_support.projection.RetainedXmpSidecar, ...] = (), projection_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
    },
    "MediaArchiveProjectionPayload": {
      "kind": "class",
      "members": {
        "canonical_members": {
          "kind": "method",
          "signature": "\"(self) -> 'Self'\""
        }
      },
      "schema_sha256": "637ed91c9b5702d779d7dfa48c011e623e38c6c7fb91ca7a4f8e01871aeafd09",
      "signature": "\"(*, format: Literal['stove0-media-archive-projection/v1'] = 'stove0-media-archive-projection/v1', observation_result_sha256s: tuple[str, ...], items: Annotated[tuple[stove0_media_archive_target_support.projection.MediaProjectionItem, ...], MinLen(min_length=1)], retained_xmp_sidecars: tuple[stove0_media_archive_target_support.projection.RetainedXmpSidecar, ...] = ()) -> None\""
    },
    "MediaProjectedValue": {
      "kind": "class",
      "members": {
        "bind_source": {
          "kind": "method",
          "signature": "\"(self) -> 'Self'\""
        },
        "canonical_evidence": {
          "kind": "classmethod",
          "signature": "\"(cls, value: 'tuple[MediaFactEvidence, ...]') -> 'tuple[MediaFactEvidence, ...]'\""
        }
      },
      "schema_sha256": "cb40d7111242d2d59c8a440788114e49f192faf2baa7e1fdeca5876548348f44",
      "signature": "\"(*, name: Literal['capture-time', 'creator', 'device-make', 'device-model', 'gps-latitude', 'gps-longitude'], value: JsonValue, source: Literal['observation', 'recipe'], evidence: tuple[stove0_media_metadata_observer_contracts.contracts.MediaFactEvidence, ...] = ()) -> None\""
    },
    "MediaProjectionItem": {
      "kind": "class",
      "members": {
        "canonical_evidence": {
          "kind": "method",
          "signature": "\"(self) -> 'Self'\""
        },
        "canonical_path": {
          "kind": "classmethod",
          "signature": "\"(cls, value: 'str') -> 'str'\""
        },
        "canonical_sidecars": {
          "kind": "classmethod",
          "signature": "\"(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'\""
        },
        "derived_from": {
          "kind": "property",
          "signature": "\"(self) -> 'tuple[str, ...]'\""
        }
      },
      "schema_sha256": "66e94ffdb8669f096b49716657bc94b8f025e37ebf2e15ac8fe28451448ede3c",
      "signature": "'(*, input_artifact_id: str, associated_sidecar_artifact_ids: tuple[str, ...] = (), archive_path: str, xmp_path: str, assertions: tuple[stove0_media_metadata_observer_contracts.contracts.MediaMetadataFact, ...] = (), selected: tuple[stove0_media_archive_target_support.projection.MediaProjectedValue, ...] = ()) -> None'"
    },
    "RetainedXmpSidecar": {
      "kind": "class",
      "members": {
        "canonical_path": {
          "kind": "classmethod",
          "signature": "\"(cls, value: 'str') -> 'str'\""
        }
      },
      "schema_sha256": "bf5e283d3e6a8fd5d7e2d364e1f8b834199f2aeeb69f4664d31be4a46bbe6923",
      "signature": "'(*, input_artifact_id: str, output_path: str) -> None'"
    },
    "ffmpeg_container_metadata_args": {
      "kind": "function",
      "signature": "\"(item: 'MediaProjectionItem') -> 'list[str]'\""
    },
    "render_projection_xmp": {
      "kind": "function",
      "signature": "\"(item: 'MediaProjectionItem', *, tags: 'Sequence[str]' = ()) -> 'bytes'\""
    },
    "resolve_media_archive_preflight_projection": {
      "kind": "function",
      "signature": "\"(request: 'TargetPreflightRequest', *, policy: 'MediaProjectionPolicy', archive_directory: 'str', archive_suffix: 'str') -> 'MediaArchiveProjection'\""
    },
    "resolve_media_archive_projection": {
      "kind": "function",
      "signature": "\"(*, inputs: 'Sequence[InputArtifact]', observations: 'Sequence[ObservationEvidence]', policy: 'MediaProjectionPolicy', archive_directory: 'str', archive_suffix: 'str') -> 'MediaArchiveProjection'\""
    }
  },
  "module": "stove0_media_archive_target_support"
}
```
