# riverhog_provenance.ProvenanceProviderMetadata.as_dict

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-provenanceprovidermet-b3925597e7:aab1e3118f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d7364b0ce5"></a>
- <a id="s-493189b9ee"></a>`distribution`: `riverhog-provenance`
- <a id="s-9a44f5964e"></a>`module`: `riverhog_provenance`
- <a id="s-6b18fb68a5"></a>`name`: `as_dict`
- <a id="s-ef8302e4a3"></a>`owner`: `riverhog_provenance.ProvenanceProviderMetadata`
- <a id="s-61a87ff1b1"></a>`unit`: `member`

### Declared structure

- <a id="s-e807761b01"></a>`kind`: `"method"`
- <a id="s-7c60c4f7cd"></a>`signature`: `"\"(self) -> 'dict[str, str \| None]'\""`

## Maintained corroboration

### Related interface records

- [riverhog_provenance.ProvenanceProviderMetadata](riverhog-provenance-provenanceprovidermetadata.md)

## Governing policies

- <a id="pa-61bc1241a5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.ProvenanceProviderMetadata.as_dict`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3c1ab00fc95261ef5f18cb2fa34d71320cb875acf19920a802c49baf7c2861c5 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'dict[str, str | None]'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "as_dict",
  "owner": "riverhog_provenance.ProvenanceProviderMetadata",
  "unit": "member"
}
```
