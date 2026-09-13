# riverhog_protocol.RetirementClaimReferenceDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-retirementclaimreferencedocument:72c7681a98 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a3b7fc9ce8"></a>
| Field | Shape |
|---|---|
| <a id="s-64fdb2342a"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-d82f2c90ad"></a>`distribution` | "riverhog-protocol" |
| <a id="s-321a1fb187"></a>`module` | "riverhog_protocol" |
| <a id="s-0a4afab5ee"></a>`name` | "RetirementClaimReferenceDocument" |
| <a id="s-e4146fce05"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.RetirementClaimReferenceDocument.validate_settlement_form](riverhog-protocol-retirementclaimreferencedocument-validate-settlement-form.md)

## Governing policies

- <a id="pa-8a65e8800b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.RetirementClaimReferenceDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2680d7451b0efaeb2af875b8da22db5004354bea049a5e68139171e3e3fb4d94 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "da00663a91a98b6162e05a14a3d0af2e0dd29dfb08e315ef2139900b1c19627e",
    "signature": "\"(*, claim_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], fence: Annotated[int, Ge(ge=1)], work_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], execution_id: Optional[Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[_PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')])]] = None, output_collection_id: CollectionId | None = None, outcomes: riverhog_protocol.collection_workflow_transport.ExactSetAuthorityDocument | None = None) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "RetirementClaimReferenceDocument",
  "unit": "export"
}
```
