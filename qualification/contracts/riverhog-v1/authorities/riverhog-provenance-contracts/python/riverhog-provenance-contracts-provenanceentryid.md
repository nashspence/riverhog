# riverhog_provenance_contracts.ProvenanceEntryId

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance-contracts:riverhog-provenance-contracts-provenanceentryid:0d8627d2c0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-46730cca18"></a>
- <a id="s-e761ffaf3b"></a>`distribution`: `riverhog-provenance-contracts`
- <a id="s-71a8fad1e2"></a>`module`: `riverhog_provenance_contracts`
- <a id="s-d092a102d2"></a>`name`: `ProvenanceEntryId`
- <a id="s-7d243a6569"></a>`unit`: `export`

### Declared structure

- <a id="s-08445201dc"></a>`kind`: `"type-alias"`
- <a id="s-3a0271eac2"></a>`value`: `"typing.Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[_PydanticGeneralMetadata(pattern='^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')]), AfterValidator(func=<function _entry_id>)]"`

## Governing policies

- <a id="pa-d12df25be9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance-contracts:riverhog_provenance_contracts](../../../evidence/sources.md#src-9b6289a988) — `packages/riverhog-provenance-contracts/src/riverhog_provenance_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance_contracts.ProvenanceEntryId`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 29f0ad6095a74e1f72cb2168f36a0730674d82ecb0892c7185ed7629e205fc90 -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[_PydanticGeneralMetadata(pattern='^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')]), AfterValidator(func=<function _entry_id>)]"
  },
  "distribution": "riverhog-provenance-contracts",
  "module": "riverhog_provenance_contracts",
  "name": "ProvenanceEntryId",
  "unit": "export"
}
```
