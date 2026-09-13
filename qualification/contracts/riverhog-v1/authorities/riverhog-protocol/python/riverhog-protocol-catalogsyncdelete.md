# riverhog_protocol.CatalogSyncDelete

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-catalogsyncdelete:b86be775c8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bf6aadfc13"></a>
| Field | Shape |
|---|---|
| <a id="s-49fabeb945"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-ccbef6b4ec"></a>`distribution` | "riverhog-protocol" |
| <a id="s-d866eb54b7"></a>`module` | "riverhog_protocol" |
| <a id="s-0a9d4140f8"></a>`name` | "CatalogSyncDelete" |
| <a id="s-86b99a8281"></a>`unit` | "export" |

## Governing policies

- <a id="pa-d3223a6e95"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CatalogSyncDelete`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7c03d9719f14e2dadec0bf40568486198a1f802a66572ff16e317671eb9bcdef -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "157cdc75cb1f1a98f27527f0afef0cbf357b327e6096118a624d7e2a4c011b7c",
    "signature": "\"(*, operation: Literal['delete'] = 'delete', collection_id: CollectionId, revision: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=19, pattern='^(?:[1-9][0-9]{0,17}|[1-8][0-9]{18})$', ascii_only=None)]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CatalogSyncDelete",
  "unit": "export"
}
```
