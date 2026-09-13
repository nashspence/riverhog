# riverhog_protocol.PortableCollectionInventoryPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-portablecollectioninventorypage:180fca9afb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6a35447804"></a>
| Field | Shape |
|---|---|
| <a id="s-904904ee58"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-802a817536"></a>`distribution` | "riverhog-protocol" |
| <a id="s-4052838d48"></a>`module` | "riverhog_protocol" |
| <a id="s-1e00a5d1ef"></a>`name` | "PortableCollectionInventoryPage" |
| <a id="s-2d41295a95"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.PortableCollectionInventoryPage.validate_page](riverhog-protocol-portablecollectioninventorypage-validate-page.md)

## Governing policies

- <a id="pa-3e89fa838e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.PortableCollectionInventoryPage`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ad91ca1477c45eb2c63dc143c1b54d96c5abd9d1b9747c1572d44550e2514c94 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "a148820574851c9df6f4de6d585e2f9e2aa24065422845276525befd8b1f06d6",
    "signature": "\"(*, format: Literal['riverhog-collection-inventory-page/v1'] = 'riverhog-collection-inventory-page/v1', authority: riverhog_protocol.portable_collection.PortableCollectionInventoryAuthority, files: Annotated[list[riverhog_protocol.file_identity.ImmutableFileIdentityDocument], MaxLen(max_length=1000)], next_cursor: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=8192)] = None, complete: bool) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "PortableCollectionInventoryPage",
  "unit": "export"
}
```
