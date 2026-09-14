# riverhog_provenance.ProvenanceRootDocument.from_json_bytes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-provenancerootdocumen-a0621991d0:60e4952bb8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a4b8de9084"></a>
- <a id="s-9de0e3ed61"></a>`distribution`: `riverhog-provenance`
- <a id="s-3ee5491cc7"></a>`module`: `riverhog_provenance`
- <a id="s-87a441108a"></a>`name`: `from_json_bytes`
- <a id="s-fb096dee02"></a>`owner`: `riverhog_provenance.ProvenanceRootDocument`
- <a id="s-548298ceba"></a>`unit`: `member`

### Declared structure

- <a id="s-43fe9a2306"></a>`kind`: `"classmethod"`
- <a id="s-07a5fb2e68"></a>`signature`: `"\"(cls, content: 'bytes') -> 'ProvenanceRootDocument'\""`

## Maintained corroboration

### Related interface records

- [riverhog_provenance.ProvenanceRootDocument](riverhog-provenance-provenancerootdocument.md)

## Governing policies

- <a id="pa-6d6272b8b1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.ProvenanceRootDocument.from_json_bytes`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5a5567fa178fbca0cb93fae0a78176ebee18536468ac06997f109629e76dda8a -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, content: 'bytes') -> 'ProvenanceRootDocument'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "from_json_bytes",
  "owner": "riverhog_provenance.ProvenanceRootDocument",
  "unit": "member"
}
```
