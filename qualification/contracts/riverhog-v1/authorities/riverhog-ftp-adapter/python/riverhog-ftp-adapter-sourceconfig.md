# riverhog_ftp_adapter.SourceConfig

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-ftp-adapter:riverhog-ftp-adapter-sourceconfig:c1aefaa4e9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-99d467f9fb"></a>
| Field | Shape |
|---|---|
| <a id="s-d47739d2d9"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-d64b48fa7d"></a>`distribution` | "riverhog-ftp-adapter" |
| <a id="s-617284a63c"></a>`module` | "riverhog_ftp_adapter" |
| <a id="s-55281d26bd"></a>`name` | "SourceConfig" |
| <a id="s-7cd33da652"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_ftp_adapter.SourceConfig.absolute_root](riverhog-ftp-adapter-sourceconfig-absolute-root.md)
- [riverhog_ftp_adapter.SourceConfig.complete_policy](riverhog-ftp-adapter-sourceconfig-complete-policy.md)

## Governing policies

- <a id="pa-ab51d7f58a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-ftp-adapter:riverhog_ftp_adapter](../../../evidence/sources.md#src-8d11f8fa97) — `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_ftp_adapter.SourceConfig`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dd0fda2f43e9a210e0f3956a2f20aba9adf7a66f7300614170d049b745a47d04 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "814e86c57a33d78f0992459b58d920b897b9707c9376143172c9d60d967cd496",
    "signature": "\"(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._-]{0,118}[a-z0-9])?$')], root: pathlib.Path, ingest_source: Annotated[str, MinLen(min_length=1), MaxLen(max_length=512)], archive_store: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=160)] = None, description: CollectionDescription | None = None, tags: tuple[CollectionTag, ...] = (), close_mode: Literal['stable', 'explicit-flush'] = 'stable', max_files: Annotated[int, Ge(ge=1)] = 1000, max_bytes: Annotated[int, Ge(ge=1)] = 107374182400, provenance: Literal['capture', 'omit'] = 'capture', provenance_omission_reason: Annotated[str | None, MaxLen(max_length=1000)] = None) -> None\""
  },
  "distribution": "riverhog-ftp-adapter",
  "module": "riverhog_ftp_adapter",
  "name": "SourceConfig",
  "unit": "export"
}
```
