# riverhog_provenance_contracts.ProvenanceJournalId

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance-contracts:riverhog-provenance-contracts-provenancejournalid:fb2d18a111 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-82a616c89e"></a>
- <a id="s-911f2e7386"></a>`distribution`: `riverhog-provenance-contracts`
- <a id="s-1977767151"></a>`module`: `riverhog_provenance_contracts`
- <a id="s-58bae5c119"></a>`name`: `ProvenanceJournalId`
- <a id="s-3d0a989c03"></a>`unit`: `export`

### Declared structure

- <a id="s-635e8e5720"></a>`kind`: `"type-alias"`
- <a id="s-fbe13fbc97"></a>`value`: `"typing.Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[_PydanticGeneralMetadata(pattern='^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')]), AfterValidator(func=<function _journal_id>)]"`

## Governing policies

- <a id="pa-cc451e92a1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance-contracts:riverhog_provenance_contracts](../../../evidence/sources.md#src-9b6289a988) — `packages/riverhog-provenance-contracts/src/riverhog_provenance_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance_contracts.ProvenanceJournalId`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e244129337fe32ec0172fe78a6e000aa5be511e6ff97ba64242aff949423828f -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[_PydanticGeneralMetadata(pattern='^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')]), AfterValidator(func=<function _journal_id>)]"
  },
  "distribution": "riverhog-provenance-contracts",
  "module": "riverhog_provenance_contracts",
  "name": "ProvenanceJournalId",
  "unit": "export"
}
```
