# riverhog_ftp_adapter.FtpAdapterConfig

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-ftp-adapter:riverhog-ftp-adapter-ftpadapterconfig:c98ed5305f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c8cff4a0dd"></a>
| Field | Shape |
|---|---|
| <a id="s-0649b5029d"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-5d1a9a7079"></a>`distribution` | "riverhog-ftp-adapter" |
| <a id="s-a0c2b9e7b5"></a>`module` | "riverhog_ftp_adapter" |
| <a id="s-dccfd93994"></a>`name` | "FtpAdapterConfig" |
| <a id="s-070e05ce3d"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_ftp_adapter.FtpAdapterConfig.provenance_authority](riverhog-ftp-adapter-ftpadapterconfig-provenance-authority.md)
- [riverhog_ftp_adapter.FtpAdapterConfig.source](riverhog-ftp-adapter-ftpadapterconfig-source.md)
- [riverhog_ftp_adapter.FtpAdapterConfig.unique_sources](riverhog-ftp-adapter-ftpadapterconfig-unique-sources.md)

## Governing policies

- <a id="pa-9dca47410a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-ftp-adapter:riverhog_ftp_adapter](../../../evidence/sources.md#src-8d11f8fa97) — `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_ftp_adapter.FtpAdapterConfig`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cc0dfb04e5283146648845771fe79803cf4acee73cbeed502ad9ae73267c14ef -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "fac016912d09d9ab92ea34f8cbc77c485ac0e29bc5dd0d03a4f3e89562e41674",
    "signature": "'(*, host_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=255)], riverhog_base_url: Annotated[str, MinLen(min_length=1), MaxLen(max_length=2048)], riverhog_token: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], allow_insecure_http: bool = False, api_token: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], provenance_observer: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=255)] = None, sources: Annotated[tuple[riverhog_ftp_adapter.config.SourceConfig, ...], MinLen(min_length=1)], poll_seconds: Annotated[float, Ge(ge=0.1), Le(le=3600)] = 5.0, pending_claim_capacity: Annotated[int, Ge(ge=1)] = 128, claim_attempt_budget: Annotated[int, Ge(ge=2)] = 8, discovery_entry_budget: Annotated[int, Ge(ge=1)] = 4096, completion_failure_capacity: Annotated[int, Ge(ge=1)] = 128, completion_failure_attempt_budget: Annotated[int, Ge(ge=1)] = 8) -> None'"
  },
  "distribution": "riverhog-ftp-adapter",
  "module": "riverhog_ftp_adapter",
  "name": "FtpAdapterConfig",
  "unit": "export"
}
```
