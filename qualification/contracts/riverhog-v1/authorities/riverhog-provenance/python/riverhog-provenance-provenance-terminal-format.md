# riverhog_provenance.PROVENANCE_TERMINAL_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-provenance-terminal-format:f6ce277f51 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-011aeed0bd"></a>
- <a id="s-2eb7b9e272"></a>`distribution`: `riverhog-provenance`
- <a id="s-6fb142e111"></a>`module`: `riverhog_provenance`
- <a id="s-44d8f328ab"></a>`name`: `PROVENANCE_TERMINAL_FORMAT`
- <a id="s-3fcc5ab36c"></a>`unit`: `export`

### Declared structure

- <a id="s-e6e913e00f"></a>`kind`: `"constant"`
- <a id="s-d9e9be9923"></a>`value`: `"riverhog-provenance-terminal/v1"`

## Governing policies

- <a id="pa-e0b5b07ffb"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources/authorities.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.PROVENANCE_TERMINAL_FORMAT`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7b6ba4e548739c61dd705d3f7e4669ed4e78c675c422f6e7ed87ea6e27438202 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "riverhog-provenance-terminal/v1"
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "PROVENANCE_TERMINAL_FORMAT",
  "unit": "export"
}
```

</details>
