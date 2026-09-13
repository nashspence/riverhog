# riverhog_protocol.CatalogSyncChangePage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-catalogsyncchangepage:dcea2a6ab3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-784433f1d0"></a>
| Field | Shape |
|---|---|
| <a id="s-8d17193348"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-98056499f6"></a>`distribution` | "riverhog-protocol" |
| <a id="s-e576aa7c08"></a>`module` | "riverhog_protocol" |
| <a id="s-9a667e7ee4"></a>`name` | "CatalogSyncChangePage" |
| <a id="s-03688897b7"></a>`unit` | "export" |

## Governing policies

- <a id="pa-49beda2f83"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CatalogSyncChangePage`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 17b141a9765eb01193fd0109a312b3cc2500c5427c8901f321a030655d1c7989 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "94d26d8fe010c743e6db365252c8393aa01f6b8fe84a47fc389091754e4c9bd1",
    "signature": "\"(*, format: Literal['riverhog-catalog-sync/v1'] = 'riverhog-catalog-sync/v1', source_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=64, max_length=64, pattern='^[0-9a-f]{64}$', ascii_only=None)], authorization_view_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=64, max_length=64, pattern='^[0-9a-f]{64}$', ascii_only=None)], changes: Annotated[list[Annotated[riverhog_protocol.catalog_sync.CatalogSyncUpsert | riverhog_protocol.catalog_sync.CatalogSyncDelete, FieldInfo(annotation=NoneType, required=True, discriminator='operation')]], MaxLen(max_length=100)], next_cursor: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=4096, pattern=None, ascii_only=None)], caught_up: bool, through_revision: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=19, pattern='^(?:0|[1-9][0-9]{0,17}|[1-8][0-9]{18})$', ascii_only=None)]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CatalogSyncChangePage",
  "unit": "export"
}
```
