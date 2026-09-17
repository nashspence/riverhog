# riverhog_provenance.PROVENANCE_TERMINAL_SCHEMA

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-provenance-terminal-schema:6a6dec59df -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fd07c2e943"></a>
- <a id="s-19d3071e6a"></a>`distribution`: `riverhog-provenance`
- <a id="s-f4508bfb31"></a>`module`: `riverhog_provenance`
- <a id="s-bea989bca5"></a>`name`: `PROVENANCE_TERMINAL_SCHEMA`
- <a id="s-0fb4f48c03"></a>`unit`: `export`

### Declared structure

- <a id="s-4df63ec14c"></a>`kind`: `"constant"`
- <a id="s-7c5d4c9b5e"></a>`value`: `"riverhog-provenance-terminal/v1"`

## Governing policies

- <a id="pa-5a70509e02"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.PROVENANCE_TERMINAL_SCHEMA`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 915babea96617e554f8adc9b0f260fcfa4f278961fd8d839e6120efdbf140884 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "riverhog-provenance-terminal/v1"
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "PROVENANCE_TERMINAL_SCHEMA",
  "unit": "export"
}
```

</details>
