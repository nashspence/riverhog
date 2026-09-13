# stove0_observer_protocol.CollectionRootRef

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-collectionrootref:758880d4c6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a54d7eda75"></a>
| Field | Shape |
|---|---|
| <a id="s-c4e3a6f7b3"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-e7d6d4ae31"></a>`distribution` | "stove0-observer-protocol" |
| <a id="s-22608d5b06"></a>`module` | "stove0_observer_protocol" |
| <a id="s-9b7ec3d380"></a>`name` | "CollectionRootRef" |
| <a id="s-e57eeb528d"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_observer_protocol.CollectionRootRef.to_identity](stove0-observer-protocol-collectionrootref-to-identity.md)
- [stove0_observer_protocol.CollectionRootRef.from_identity](stove0-observer-protocol-collectionrootref-from-identity.md)

## Governing policies

- <a id="pa-9e1fe6c93a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.CollectionRootRef`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 99efd5a3affadb7ed1994ff41d85c965879d67122d9b00184b0fc359886c20b1 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "0129b21c7d9e83eb25d287865778230c2388dd72c6a5fa8243a72b6c74a72649",
    "signature": "\"(*, collection_id: CollectionId, archive_root_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], content_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "CollectionRootRef",
  "unit": "export"
}
```
