# riverhog_protocol.CollectionUploadProvenanceJournalStatusDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionuploadprovena-39769e4d6c:a198a9296c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cc38a2e4c5"></a>
| Field | Shape |
|---|---|
| <a id="s-5ec37b7f6d"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-d195a089ff"></a>`distribution` | "riverhog-protocol" |
| <a id="s-ad8af71d6b"></a>`module` | "riverhog_protocol" |
| <a id="s-94fa29b8f6"></a>`name` | "CollectionUploadProvenanceJournalStatusDocument" |
| <a id="s-04174ca703"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.CollectionUploadProvenanceJournalStatusDocument.validate_progress](riverhog-protocol-collectionuploadprovenancejournalstatusdocument-validate-progress.md)

## Governing policies

- <a id="pa-2e71d63453"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadProvenanceJournalStatusDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d29687a2a080a8f288bc5be5b3a36bceadefbce274d52ee34be6cdad72d1b5fa -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "58d9abee3be096444e28e38c250957e0808e2af0dbe6cac127f0955006c44831",
    "signature": "\"(*, journal_id: ProvenanceJournalId, state: Literal['accepting', 'validating', 'sealed', 'failed'], bytes: Annotated[int, Strict(strict=True), Ge(ge=1)], sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], accepted_bytes: Annotated[int, Strict(strict=True), Ge(ge=0)], failure: str | None = None, current_state_id: ProvenanceStateId | None = None, current_path: str | None = None, current_bytes: Annotated[int | None, Strict(strict=True), Ge(ge=0)] = None, current_sha256: Optional[Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[_PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')])]] = None) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionUploadProvenanceJournalStatusDocument",
  "unit": "export"
}
```
