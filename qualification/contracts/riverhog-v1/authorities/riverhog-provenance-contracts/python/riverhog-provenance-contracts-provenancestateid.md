# riverhog_provenance_contracts.ProvenanceStateId

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance-contracts:riverhog-provenance-contracts-provenancestateid:929b0ac1bd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-af0da57744"></a>
- <a id="s-557b6c51b5"></a>`distribution`: `riverhog-provenance-contracts`
- <a id="s-470f52ec52"></a>`module`: `riverhog_provenance_contracts`
- <a id="s-c693f7e153"></a>`name`: `ProvenanceStateId`
- <a id="s-902c17074f"></a>`unit`: `export`

### Declared structure

- <a id="s-85ad7fee6d"></a>`kind`: `"type-alias"`
- <a id="s-86d15913ea"></a>`value`: `"typing.Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[_PydanticGeneralMetadata(pattern='^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')]), AfterValidator(func=<function _state_id>)]"`

## Governing policies

- <a id="pa-4c3f33845c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance-contracts:riverhog_provenance_contracts](../../../evidence/sources.md#src-9b6289a988) — `packages/riverhog-provenance-contracts/src/riverhog_provenance_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance_contracts.ProvenanceStateId`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d8c6bf96e55be90e62c8ff203fd6b0adc865a4b46ccdcdc02c9c78d4b55772b6 -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[_PydanticGeneralMetadata(pattern='^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')]), AfterValidator(func=<function _state_id>)]"
  },
  "distribution": "riverhog-provenance-contracts",
  "module": "riverhog_provenance_contracts",
  "name": "ProvenanceStateId",
  "unit": "export"
}
```

</details>
