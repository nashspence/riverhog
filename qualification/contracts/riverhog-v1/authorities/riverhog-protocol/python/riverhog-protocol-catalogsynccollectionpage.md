# riverhog_protocol.CatalogSyncCollectionPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-catalogsynccollectionpage:b333e98a32 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-670992d616"></a>
| Field | Shape |
|---|---|
| <a id="s-59efb37831"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-687dba8f0b"></a>`distribution` | "riverhog-protocol" |
| <a id="s-786f21b3dc"></a>`module` | "riverhog_protocol" |
| <a id="s-f611e36554"></a>`name` | "CatalogSyncCollectionPage" |
| <a id="s-226d2c8593"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.CatalogSyncCollectionPage.validate_continuation](riverhog-protocol-catalogsynccollectionpage-validate-continuation.md)

## Governing policies

- <a id="pa-9ed2e7ae86"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CatalogSyncCollectionPage`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 29c29ca3e7978708c3561482a94168bb819590d636be8d085c77cd7cd3735919 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "d5bc608ea400e9446d118d792c582cfd63d74b0421fca734919a10b28e359bf8",
    "signature": "\"(*, format: Literal['riverhog-catalog-sync/v1'] = 'riverhog-catalog-sync/v1', source_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=64, max_length=64, pattern='^[0-9a-f]{64}$', ascii_only=None)], authorization_view_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=64, max_length=64, pattern='^[0-9a-f]{64}$', ascii_only=None)], collections: Annotated[list[riverhog_protocol.catalog_sync.CatalogSyncDescriptor], MaxLen(max_length=100)], next_cursor: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=4096, pattern=None, ascii_only=None)]] = None, changes_cursor: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=4096, pattern=None, ascii_only=None)]] = None) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CatalogSyncCollectionPage",
  "unit": "export"
}
```
