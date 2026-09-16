# riverhog_provenance.parse_journal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-parse-journal:d09ac9627f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ede6dd5cb6"></a>
- <a id="s-9ede39704b"></a>`distribution`: `riverhog-provenance`
- <a id="s-cbe808f1be"></a>`module`: `riverhog_provenance`
- <a id="s-9829183b98"></a>`name`: `parse_journal`
- <a id="s-2dd09cb9c8"></a>`unit`: `export`

### Declared structure

- <a id="s-5abcd55d05"></a>`kind`: `"function"`
- <a id="s-48971ffdf8"></a>`signature`: `"\"(content: 'bytes') -> 'tuple[JournalFrame, ...]'\""`

## Governing policies

- <a id="pa-6fab4fd9b2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.parse_journal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2e300f55f010d0a388db386b56db49d398e3a1648ac2af74a0e4185e5afb31d8 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(content: 'bytes') -> 'tuple[JournalFrame, ...]'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "parse_journal",
  "unit": "export"
}
```

</details>
