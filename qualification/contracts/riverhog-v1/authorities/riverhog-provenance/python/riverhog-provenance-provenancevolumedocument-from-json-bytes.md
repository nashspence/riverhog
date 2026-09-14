# riverhog_provenance.ProvenanceVolumeDocument.from_json_bytes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-provenancevolumedocum-0c8916d07c:d922c495d0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ecc2177617"></a>
- <a id="s-e183de038d"></a>`distribution`: `riverhog-provenance`
- <a id="s-3fe24f7dd5"></a>`module`: `riverhog_provenance`
- <a id="s-2cc62dcc50"></a>`name`: `from_json_bytes`
- <a id="s-accc479bee"></a>`owner`: `riverhog_provenance.ProvenanceVolumeDocument`
- <a id="s-77c3d218bc"></a>`unit`: `member`

### Declared structure

- <a id="s-9fdbc6b001"></a>`kind`: `"classmethod"`
- <a id="s-71dd1c7055"></a>`signature`: `"\"(cls, content: 'bytes') -> 'ProvenanceVolumeDocument'\""`

## Maintained corroboration

### Related interface records

- [riverhog_provenance.ProvenanceVolumeDocument](riverhog-provenance-provenancevolumedocument.md)

## Governing policies

- <a id="pa-ad0807ec0a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.ProvenanceVolumeDocument.from_json_bytes`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e3973de08da83b13d6a92b56a2e98a4838aefa4627a54a4307af9d89ff66c753 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, content: 'bytes') -> 'ProvenanceVolumeDocument'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "from_json_bytes",
  "owner": "riverhog_provenance.ProvenanceVolumeDocument",
  "unit": "member"
}
```
