# riverhog_provenance_contracts.ProvenanceContractBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance-contracts:riverhog-provenance-contracts-provenancec-a09835e7cf:6355183749 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-eec57be6e3"></a>
- <a id="s-7a662c14a0"></a>`distribution`: `riverhog-provenance-contracts`
- <a id="s-061aa03156"></a>`module`: `riverhog_provenance_contracts`
- <a id="s-029457a1af"></a>`name`: `ProvenanceContractBinding`
- <a id="s-b393fc0f69"></a>`unit`: `export`

### Declared structure

- <a id="s-dc89cc68e2"></a>`kind`: `"class"`
- <a id="s-d8cc76985e"></a>`signature`: `"\"(*, contract_id: 'str', schemas: 'Iterable[Mapping[str, Any]]') -> 'None'\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-fbd405824c"></a>`format` | `'str'` | `required` |
| <a id="s-dc876e5b6d"></a>`contract_id` | `'str'` | `required` |
| <a id="s-0852df3c78"></a>`contract_sha256` | `'str'` | `required` |
| <a id="s-f935ed9fcd"></a>`schema_dialect` | `'str'` | `required` |
| <a id="s-56245cc4ca"></a>`format_policy` | `'str'` | `required` |
| <a id="s-3bd1e79564"></a>`_schemas_json` | `'bytes'` | `required` |

## Maintained corroboration

### Related interface records

- [reference](riverhog-provenance-contracts-provenancecontractbinding-reference.md)
- [schemas](riverhog-provenance-contracts-provenancecontractbinding-schemas.md)

## Governing policies

- <a id="pa-c158e105c8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance-contracts:riverhog_provenance_contracts](../../../evidence/sources.md#src-9b6289a988) — `packages/riverhog-provenance-contracts/src/riverhog_provenance_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance_contracts.ProvenanceContractBinding`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4671bdf6b780a94f160b6f2818b30f29da4c32a87c6ce441ef70aa645bf572a3 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "format",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "contract_id",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "contract_sha256",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "schema_dialect",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "format_policy",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "_schemas_json",
        "type": "'bytes'"
      }
    ],
    "kind": "class",
    "signature": "\"(*, contract_id: 'str', schemas: 'Iterable[Mapping[str, Any]]') -> 'None'\""
  },
  "distribution": "riverhog-provenance-contracts",
  "module": "riverhog_provenance_contracts",
  "name": "ProvenanceContractBinding",
  "unit": "export"
}
```
