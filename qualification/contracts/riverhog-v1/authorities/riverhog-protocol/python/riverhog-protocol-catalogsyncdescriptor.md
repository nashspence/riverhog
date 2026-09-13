# riverhog_protocol.CatalogSyncDescriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-catalogsyncdescriptor:eab7a09e06 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-56ec8b3127"></a>
| Field | Shape |
|---|---|
| <a id="s-6dc42e93ab"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-85d3c30427"></a>`distribution` | "riverhog-protocol" |
| <a id="s-3cc6fcb805"></a>`module` | "riverhog_protocol" |
| <a id="s-d71acd22c8"></a>`name` | "CatalogSyncDescriptor" |
| <a id="s-d5d674c0a1"></a>`unit` | "export" |

## Governing policies

- <a id="pa-e0342cb6f9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CatalogSyncDescriptor`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a5ddc083cb602b3e6fc540488226c89eaa90758998a270952bb8f418d472379d -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "7018dac67ff6d17ceb020510e1c981a49257057bdc9b2efdc556f960d83fef8a",
    "signature": "\"(*, collection_id: CollectionId, archive_root_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=64, max_length=64, pattern='^[0-9a-f]{64}$', ascii_only=None)], content_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=64, max_length=64, pattern='^[0-9a-f]{64}$', ascii_only=None)], description: CollectionDescription | None, description_revision: Annotated[int, Strict(strict=True), Ge(ge=0), Le(le=9007199254740991)], description_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=64, max_length=64, pattern='^[0-9a-f]{64}$', ascii_only=None)], tag_revision: Annotated[int, Strict(strict=True), Ge(ge=1), Le(le=9007199254740991)], tag_set_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=64, max_length=64, pattern='^[0-9a-f]{64}$', ascii_only=None)], revision: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=19, pattern='^(?:[1-9][0-9]{0,17}|[1-8][0-9]{18})$', ascii_only=None)]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CatalogSyncDescriptor",
  "unit": "export"
}
```
