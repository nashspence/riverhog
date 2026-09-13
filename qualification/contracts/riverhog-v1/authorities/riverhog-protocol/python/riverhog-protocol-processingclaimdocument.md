# riverhog_protocol.ProcessingClaimDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingclaimdocument:26bbd50563 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-75863d0da4"></a>
| Field | Shape |
|---|---|
| <a id="s-753533f2c7"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-3a9e8ca942"></a>`distribution` | "riverhog-protocol" |
| <a id="s-0c651f9b71"></a>`module` | "riverhog_protocol" |
| <a id="s-17efc6ff7d"></a>`name` | "ProcessingClaimDocument" |
| <a id="s-1118c39165"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.ProcessingClaimDocument.validate_claim](riverhog-protocol-processingclaimdocument-validate-claim.md)

## Governing policies

- <a id="pa-f88ab499ee"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingClaimDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: be701d3a9561206de05489ee933e2fcb590fe00947579c0bd644ae62cae2ad8d -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "9d71d3fac376c7ce173117cd7c605bcc77e55c047d52aff20a031799978d69c9",
    "signature": "\"(*, format: Literal['riverhog-processing-claim/v1'], id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], work_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], consumer: riverhog_protocol.collection_workflow_transport.ProcessingClaimConsumerDocument, purpose: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], state: Literal['active', 'settled', 'retiring', 'abandoned', 'released'], fence: Annotated[int, Ge(ge=1)], expires_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=64)], created_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=64)], updated_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=64)], settled_at: Optional[Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[MinLen(min_length=1), MaxLen(max_length=64)])]] = None, abandoned_at: Optional[Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[MinLen(min_length=1), MaxLen(max_length=64)])]] = None, abandonment_reason: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=1000)] = None, released_at: Optional[Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[MinLen(min_length=1), MaxLen(max_length=64)])]] = None, output_collection_id: CollectionId | None = None, work_document: dict[str, typing.Any], work_document_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], inputs: riverhog_protocol.collection_workflow_transport.ReceivingSetDocument, plan: riverhog_protocol.collection_workflow_transport.ProcessingClaimPlanDocument | None = None, outcomes: riverhog_protocol.collection_workflow_transport.OutcomeSetDocument, outcome_settlement: riverhog_protocol.collection_workflow_transport.ProcessingClaimOutcomeSettlementDocument | None = None) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ProcessingClaimDocument",
  "unit": "export"
}
```
