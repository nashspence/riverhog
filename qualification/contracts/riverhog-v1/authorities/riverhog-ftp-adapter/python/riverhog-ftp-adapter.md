# riverhog_ftp_adapter

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-ftp-adapter:riverhog-ftp-adapter:d2c9eb8b9d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [Python](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-8df1d7dafb"></a>
| Field | Shape |
|---|---|
| <a id="s-769b609aca"></a>`candidate_id` | "python:riverhog-ftp-adapter:riverhog_ftp_adapter" |
| <a id="s-4ab62c914e"></a>`distribution` | "riverhog-ftp-adapter" |
| <a id="s-98934cddec"></a>`exports` | additional keys=`FtpAdapter`, `FtpAdapterConfig`, `SourceConfig`, `load_config` |
| <a id="s-6348c206b8"></a>`module` | "riverhog_ftp_adapter" |

## Governing policies

- <a id="pa-6510ce158d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-ftp-adapter:riverhog_ftp_adapter](../../../evidence/sources.md#src-8d11f8fa97) — `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/__init__.py`

### Machine authority

- `/external_contract/python/17`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a0dc4a32b9f23e76ef96eb87f9620fbd97d2d1c98f1f95e42b45cc21ea7ae003 -->

```json
{
  "candidate_id": "python:riverhog-ftp-adapter:riverhog_ftp_adapter",
  "distribution": "riverhog-ftp-adapter",
  "exports": {
    "FtpAdapter": {
      "kind": "class",
      "members": {
        "accept_completed_file": {
          "kind": "method",
          "signature": "\"(self, source: 'SourceConfig', path: 'Path', *, relative_path: 'str', source_event_id: 'str', expected_bytes: 'int', expected_sha256: 'str', provenance: 'Mapping[str, object] | None' = None, provenance_journals: 'Mapping[str, bytes] | None' = None) -> 'ProducedCollection'\""
        },
        "flush": {
          "kind": "method",
          "signature": "\"(self, source_id: 'str') -> 'dict[str, object]'\""
        },
        "run_once": {
          "kind": "method",
          "signature": "\"(self, source_ids: 'Sequence[str] | None' = None) -> 'dict[str, object]'\""
        },
        "status": {
          "kind": "method",
          "signature": "\"(self, *, page_size: 'int' = 25, page_token: 'str | None' = None) -> 'dict[str, object]'\""
        }
      },
      "signature": "\"(api: 'ApiClient', config: 'FtpAdapterConfig', *, provenance_observer_factory: 'FileStateObserverFactory | None' = None) -> 'None'\""
    },
    "FtpAdapterConfig": {
      "kind": "class",
      "members": {
        "provenance_authority": {
          "kind": "method",
          "signature": "\"(self) -> 'Self'\""
        },
        "source": {
          "kind": "method",
          "signature": "\"(self, source_id: 'str') -> 'SourceConfig'\""
        },
        "unique_sources": {
          "kind": "classmethod",
          "signature": "\"(cls, value: 'tuple[SourceConfig, ...]') -> 'tuple[SourceConfig, ...]'\""
        }
      },
      "schema_sha256": "fac016912d09d9ab92ea34f8cbc77c485ac0e29bc5dd0d03a4f3e89562e41674",
      "signature": "'(*, host_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=255)], riverhog_base_url: Annotated[str, MinLen(min_length=1), MaxLen(max_length=2048)], riverhog_token: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], allow_insecure_http: bool = False, api_token: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], provenance_observer: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=255)] = None, sources: Annotated[tuple[riverhog_ftp_adapter.config.SourceConfig, ...], MinLen(min_length=1)], poll_seconds: Annotated[float, Ge(ge=0.1), Le(le=3600)] = 5.0, pending_claim_capacity: Annotated[int, Ge(ge=1)] = 128, claim_attempt_budget: Annotated[int, Ge(ge=2)] = 8, discovery_entry_budget: Annotated[int, Ge(ge=1)] = 4096, completion_failure_capacity: Annotated[int, Ge(ge=1)] = 128, completion_failure_attempt_budget: Annotated[int, Ge(ge=1)] = 8) -> None'"
    },
    "SourceConfig": {
      "kind": "class",
      "members": {
        "absolute_root": {
          "kind": "classmethod",
          "signature": "\"(cls, value: 'Path') -> 'Path'\""
        },
        "complete_policy": {
          "kind": "method",
          "signature": "\"(self) -> 'Self'\""
        }
      },
      "schema_sha256": "814e86c57a33d78f0992459b58d920b897b9707c9376143172c9d60d967cd496",
      "signature": "\"(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._-]{0,118}[a-z0-9])?$')], root: pathlib.Path, ingest_source: Annotated[str, MinLen(min_length=1), MaxLen(max_length=512)], archive_store: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=160)] = None, description: CollectionDescription | None = None, tags: tuple[CollectionTag, ...] = (), close_mode: Literal['stable', 'explicit-flush'] = 'stable', max_files: Annotated[int, Ge(ge=1)] = 1000, max_bytes: Annotated[int, Ge(ge=1)] = 107374182400, provenance: Literal['capture', 'omit'] = 'capture', provenance_omission_reason: Annotated[str | None, MaxLen(max_length=1000)] = None) -> None\""
    },
    "load_config": {
      "kind": "function",
      "signature": "\"(path: 'Path | None' = None) -> 'FtpAdapterConfig'\""
    }
  },
  "module": "riverhog_ftp_adapter"
}
```
