# stove0_protocol.CollectionRootRef

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-collectionrootref:fc7187931c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-10a2a32df8"></a>
| Field | Shape |
|---|---|
| <a id="s-599445df4e"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-0246f8a2d7"></a>`distribution` | "stove0-protocol" |
| <a id="s-9da9fbb81a"></a>`module` | "stove0_protocol" |
| <a id="s-a7ea5e1aa7"></a>`name` | "CollectionRootRef" |
| <a id="s-ca40bdeee3"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.CollectionRootRef.from_identity](stove0-protocol-collectionrootref-from-identity.md)
- [stove0_protocol.CollectionRootRef.to_identity](stove0-protocol-collectionrootref-to-identity.md)

## Governing policies

- <a id="pa-5e315efac0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.CollectionRootRef`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 54e825fbe0ff992b26584609dfd4c33631ff7430a68cf993d5eb1293608647e8 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "0129b21c7d9e83eb25d287865778230c2388dd72c6a5fa8243a72b6c74a72649",
    "signature": "\"(*, collection_id: CollectionId, archive_root_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], content_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "CollectionRootRef",
  "unit": "export"
}
```
