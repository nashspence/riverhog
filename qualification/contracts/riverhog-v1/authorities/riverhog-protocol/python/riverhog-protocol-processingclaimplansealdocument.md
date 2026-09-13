# riverhog_protocol.ProcessingClaimPlanSealDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingclaimplansealdocument:87a3413aae -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-071ecf24c0"></a>
| Field | Shape |
|---|---|
| <a id="s-0afd73e868"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-f320099d6d"></a>`distribution` | "riverhog-protocol" |
| <a id="s-ef581ea454"></a>`module` | "riverhog_protocol" |
| <a id="s-4ba730e043"></a>`name` | "ProcessingClaimPlanSealDocument" |
| <a id="s-73111f7c5a"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.ProcessingClaimPlanSealDocument.validate_plan](riverhog-protocol-processingclaimplansealdocument-validate-plan.md)

## Governing policies

- <a id="pa-66241af7b9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingClaimPlanSealDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f6686baae605a6b15cc7ed9b2ca8f5a7aaea14f7631ea321f6a7e25d7d5265a5 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "82794bf5f22623cf0b02535380c1f3d2dcd94c0857007c80ffc82c2c87ceb392",
    "signature": "\"(*, fence: Annotated[int, Ge(ge=1)], execution_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], controller_evidence: dict[str, typing.Any], controller_evidence_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], operation: riverhog_protocol.collection_workflow_transport.OperationIdentityDocument, retirement_policy: Literal['retain', 'retire-after-verified-output'] = 'retain', retirement_grace_seconds: Annotated[int, Ge(ge=0)] = 0) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ProcessingClaimPlanSealDocument",
  "unit": "export"
}
```
