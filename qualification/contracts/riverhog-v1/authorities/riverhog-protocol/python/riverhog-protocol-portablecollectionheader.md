# riverhog_protocol.PortableCollectionHeader

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-portablecollectionheader:a4e4fc7e72 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9b00c2ff9f"></a>
| Field | Shape |
|---|---|
| <a id="s-6b7e745642"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-b87c803e8a"></a>`distribution` | "riverhog-protocol" |
| <a id="s-6d782a0492"></a>`module` | "riverhog_protocol" |
| <a id="s-cbd8df3d12"></a>`name` | "PortableCollectionHeader" |
| <a id="s-72fd8c0e01"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.PortableCollectionHeader.validate_provenance_binding](riverhog-protocol-portablecollectionheader-validate-provenance-binding.md)

## Governing policies

- <a id="pa-281326bbe1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.PortableCollectionHeader`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 88d81b3ed7f91e5e48e76d2de888f51fa001989a9e077608c8771a9aebd52040 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "f3306150c0b20a86e2053a1c71f6653c3327b4e6009f6e31027b42ae26ad46cf",
    "signature": "\"(*, format: Literal['riverhog-collection/v1'] = 'riverhog-collection/v1', collection: CollectionId, content_identity: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], encryption_format: Annotated[str, MinLen(min_length=1)], passphrase_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[A-Za-z0-9_-]{16,128}$')], provenance_mode: Literal['captured', 'mixed', 'omitted'], provenance_identity: Annotated[str | None, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')] = None) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "PortableCollectionHeader",
  "unit": "export"
}
```
