# riverhog_provenance.validate_entry_document

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-validate-entry-document:93e0771322 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d2443921e7"></a>
- <a id="s-fd7e0d3580"></a>`distribution`: `riverhog-provenance`
- <a id="s-17b8e8b85e"></a>`module`: `riverhog_provenance`
- <a id="s-ce0459897b"></a>`name`: `validate_entry_document`
- <a id="s-35eb10bda6"></a>`unit`: `export`

### Declared structure

- <a id="s-25ac06e6f6"></a>`kind`: `"function"`
- <a id="s-5b4dbbaebd"></a>`signature`: `"\"(document: 'Mapping[str, Any]') -> 'None'\""`

## Governing policies

- <a id="pa-47846b91aa"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.validate_entry_document`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f78c9882c62aa6e22d8717dc4b277f320bf50039181e3d01d36727217d7bdfe7 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(document: 'Mapping[str, Any]') -> 'None'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "validate_entry_document",
  "unit": "export"
}
```

</details>
