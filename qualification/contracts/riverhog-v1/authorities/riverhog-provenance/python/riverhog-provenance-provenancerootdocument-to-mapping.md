# riverhog_provenance.ProvenanceRootDocument.to_mapping

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-provenancerootdocumen-ad924b7148:22c2e9243e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e5f44b1787"></a>
- <a id="s-322b1d8e4a"></a>`distribution`: `riverhog-provenance`
- <a id="s-05a5b7a007"></a>`module`: `riverhog_provenance`
- <a id="s-5d46a448e0"></a>`name`: `to_mapping`
- <a id="s-53b7ce3f82"></a>`owner`: `riverhog_provenance.ProvenanceRootDocument`
- <a id="s-c435be5a1d"></a>`unit`: `member`

### Declared structure

- <a id="s-ca40055ef8"></a>`kind`: `"method"`
- <a id="s-623c1b2d72"></a>`signature`: `"\"(self) -> 'dict[str, object]'\""`

## Maintained corroboration

### Related interface records

- [ProvenanceRootDocument](riverhog-provenance-provenancerootdocument.md)

## Governing policies

- <a id="pa-ff84131980"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources/authorities.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.ProvenanceRootDocument.to_mapping`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 16555ae58db2469f518a1f350caca4eaa80ff204c45dba9145e09fdfed3bb668 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'dict[str, object]'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "to_mapping",
  "owner": "riverhog_provenance.ProvenanceRootDocument",
  "unit": "member"
}
```

</details>
