# riverhog_protocol.ProcessingClaimCreateDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingclaimcreatedocument:d17840f395 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-425c1feb0b"></a>
| Field | Shape |
|---|---|
| <a id="s-84f9f426ea"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-27797de284"></a>`distribution` | "riverhog-protocol" |
| <a id="s-e511f932c5"></a>`module` | "riverhog_protocol" |
| <a id="s-0d5d113d4f"></a>`name` | "ProcessingClaimCreateDocument" |
| <a id="s-317a90dfc4"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.ProcessingClaimCreateDocument.validate_claim](riverhog-protocol-processingclaimcreatedocument-validate-claim.md)

## Governing policies

- <a id="pa-835b9b4a19"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingClaimCreateDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2e941923bf477668814c367c950dcb028615748a295b068816b0faca9c818552 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "7f81f54843c68bd43b662283b523cf02085ec0e8d673cd66cdb5201223ea00a6",
    "signature": "\"(*, work_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], work_document: dict[str, typing.Any], work_document_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], lease_seconds: Annotated[int, Ge(ge=30), Le(le=86400)] = 1800, purpose: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)] = 'collection-work/v1') -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ProcessingClaimCreateDocument",
  "unit": "export"
}
```
