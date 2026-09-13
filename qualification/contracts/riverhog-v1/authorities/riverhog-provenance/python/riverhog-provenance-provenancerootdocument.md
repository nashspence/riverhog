# riverhog_provenance.ProvenanceRootDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-provenancerootdocument:e3512678c4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-55795d3dbe"></a>
| Field | Shape |
|---|---|
| <a id="s-b3c1643571"></a>`contract` | additional keys=`fields`, `kind`, `signature` |
| <a id="s-19190167d4"></a>`distribution` | "riverhog-provenance" |
| <a id="s-dae7a72ff5"></a>`module` | "riverhog_provenance" |
| <a id="s-51dfd3886b"></a>`name` | "ProvenanceRootDocument" |
| <a id="s-1f05b97362"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_provenance.ProvenanceRootDocument.from_json_bytes](riverhog-provenance-provenancerootdocument-from-json-bytes.md)
- [riverhog_provenance.ProvenanceRootDocument.to_mapping](riverhog-provenance-provenancerootdocument-to-mapping.md)
- [riverhog_provenance.ProvenanceRootDocument.to_json_bytes](riverhog-provenance-provenancerootdocument-to-json-bytes.md)
- [riverhog_provenance.ProvenanceRootDocument.identity](riverhog-provenance-provenancerootdocument-identity.md)

## Governing policies

- <a id="pa-a2abc96915"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.ProvenanceRootDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9e7830e5c365a31c45f62aa5a3a0fc5cc8bbf6fddd64193f4d7e98dc48f5b219 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "archive_generation",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "archive_tree_sha256",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "ordered_volume_sha256",
        "type": "'str'"
      },
      {
        "default": "'riverhog-provenance-root/v1'",
        "name": "schema",
        "type": "'str'"
      }
    ],
    "kind": "class",
    "signature": "\"(archive_generation: 'str', archive_tree_sha256: 'str', ordered_volume_sha256: 'str', schema: 'str' = 'riverhog-provenance-root/v1') -> None\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "ProvenanceRootDocument",
  "unit": "export"
}
```
