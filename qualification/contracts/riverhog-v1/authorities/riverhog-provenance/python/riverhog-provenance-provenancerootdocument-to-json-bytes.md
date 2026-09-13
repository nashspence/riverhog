# riverhog_provenance.ProvenanceRootDocument.to_json_bytes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-provenancerootdocumen-ec2f53a37e:c90769f086 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c6b0374e0b"></a>
| Field | Shape |
|---|---|
| <a id="s-dadd2fbb8c"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-a4e1e837b0"></a>`distribution` | "riverhog-provenance" |
| <a id="s-ee8ecca9e5"></a>`module` | "riverhog_provenance" |
| <a id="s-13ec44a061"></a>`name` | "to_json_bytes" |
| <a id="s-4f6e37e753"></a>`owner` | "riverhog_provenance.ProvenanceRootDocument" |
| <a id="s-a6ca710c77"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_provenance.ProvenanceRootDocument](riverhog-provenance-provenancerootdocument.md)

## Governing policies

- <a id="pa-1a7742083b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.ProvenanceRootDocument.to_json_bytes`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6b241680c4ed8ef466d1110ef5f207814bb3c368ca3fcc5c27aea663c3f81d7e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'bytes'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "to_json_bytes",
  "owner": "riverhog_provenance.ProvenanceRootDocument",
  "unit": "member"
}
```
