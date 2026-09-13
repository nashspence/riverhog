# riverhog_protocol.PortableCollectionInventoryAuthority

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-portablecollectioninven-cb5712bb2b:c7a58f0a5c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d63ed8a4d1"></a>
| Field | Shape |
|---|---|
| <a id="s-06fcc30b43"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-0639af0917"></a>`distribution` | "riverhog-protocol" |
| <a id="s-7722dce441"></a>`module` | "riverhog_protocol" |
| <a id="s-b58fa2119e"></a>`name` | "PortableCollectionInventoryAuthority" |
| <a id="s-1f229d4eb9"></a>`unit` | "export" |

## Governing policies

- <a id="pa-895b4321e4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.PortableCollectionInventoryAuthority`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ef77811f8abfe0fcac09b445f67d89f5a0319ca6422759cd49b2630f8fd89d7d -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "838903a345e2936309281466107dd5a82be450bbfab2fdfa7dc3c3710f5a16bd",
    "signature": "\"(*, header: riverhog_protocol.portable_collection.PortableCollectionHeader, inventory_identity: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], file_count: Annotated[int, Ge(ge=1)], file_bytes: Annotated[int, Ge(ge=0)]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "PortableCollectionInventoryAuthority",
  "unit": "export"
}
```
