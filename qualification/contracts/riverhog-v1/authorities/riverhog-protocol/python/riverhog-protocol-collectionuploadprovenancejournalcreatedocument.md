# riverhog_protocol.CollectionUploadProvenanceJournalCreateDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionuploadprovena-7519864da6:5de634bc4c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d7b1943ecd"></a>
| Field | Shape |
|---|---|
| <a id="s-f170f90846"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-62e4cb2e55"></a>`distribution` | "riverhog-protocol" |
| <a id="s-eeef49e8cb"></a>`module` | "riverhog_protocol" |
| <a id="s-b857ba0be3"></a>`name` | "CollectionUploadProvenanceJournalCreateDocument" |
| <a id="s-4627a295f9"></a>`unit` | "export" |

## Governing policies

- <a id="pa-357a4da043"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadProvenanceJournalCreateDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 97ebfe02291f35f26d2d5a4a87aa9986baed9c5510e45411569dcbd17e5f7d2b -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "44d861666e69c14f866d6b8948c4fc7a9c7a1534b331276b15a5ec2117e26cf9",
    "signature": "\"(*, bytes: Annotated[int, Strict(strict=True), Ge(ge=1)], sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionUploadProvenanceJournalCreateDocument",
  "unit": "export"
}
```
