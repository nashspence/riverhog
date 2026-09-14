# riverhog_provenance.parse_provenance_sequence

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-parse-provenance-sequence:1d2b4f4148 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b87ef5c04d"></a>
- <a id="s-57fba4310c"></a>`distribution`: `riverhog-provenance`
- <a id="s-7ae5525acc"></a>`module`: `riverhog_provenance`
- <a id="s-ae66ab716e"></a>`name`: `parse_provenance_sequence`
- <a id="s-89fd037c37"></a>`unit`: `export`

### Declared structure

- <a id="s-b3b1faffcf"></a>`kind`: `"function"`
- <a id="s-24c0c825c2"></a>`signature`: `"\"(value: 'object', label: 'str' = 'provenance sequence') -> 'int'\""`

## Governing policies

- <a id="pa-4b5a29fa6e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.parse_provenance_sequence`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 21d87d53561b9717080ec00dfafd01216a7073d8f653391befb212f587fd7458 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'object', label: 'str' = 'provenance sequence') -> 'int'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "parse_provenance_sequence",
  "unit": "export"
}
```
