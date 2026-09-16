# riverhog_provenance.ProvenanceVolumeDocument.metadata_path

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-provenancevolumedocum-b83c8abb28:80da89e34f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-46ee27f984"></a>
- <a id="s-566eb9d307"></a>`distribution`: `riverhog-provenance`
- <a id="s-9ed986ebd0"></a>`module`: `riverhog_provenance`
- <a id="s-943ea9e918"></a>`name`: `metadata_path`
- <a id="s-bc2d028e69"></a>`owner`: `riverhog_provenance.ProvenanceVolumeDocument`
- <a id="s-d74e7a63e1"></a>`unit`: `member`

### Declared structure

- <a id="s-edb9a49273"></a>`kind`: `"property"`
- <a id="s-3fc9cadc20"></a>`signature`: `"\"(self) -> 'str'\""`

## Maintained corroboration

### Related interface records

- [ProvenanceVolumeDocument](riverhog-provenance-provenancevolumedocument.md)

## Governing policies

- <a id="pa-ab35f2f9a1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.ProvenanceVolumeDocument.metadata_path`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4793819b9c9227f0ff4ba2239e90967220fb0257e3c0cc616045f6d7abf2bfef -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> 'str'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "metadata_path",
  "owner": "riverhog_provenance.ProvenanceVolumeDocument",
  "unit": "member"
}
```

</details>
