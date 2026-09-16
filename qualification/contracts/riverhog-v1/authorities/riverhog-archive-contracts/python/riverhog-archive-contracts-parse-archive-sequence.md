# riverhog_archive_contracts.parse_archive_sequence

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-parse-archive-sequence:76a3cad12f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9f1c62e3fd"></a>
- <a id="s-6708343c6e"></a>`distribution`: `riverhog-archive-contracts`
- <a id="s-db05cdf30d"></a>`module`: `riverhog_archive_contracts`
- <a id="s-2e5e27ef1a"></a>`name`: `parse_archive_sequence`
- <a id="s-764b027394"></a>`unit`: `export`

### Declared structure

- <a id="s-a447b4cc61"></a>`kind`: `"function"`
- <a id="s-6d9cf28a01"></a>`signature`: `"\"(value: 'object', label: 'str' = 'archive sequence') -> 'int'\""`

## Governing policies

- <a id="pa-0d801346c3"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources.md#src-4557222ddc) — `packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.parse_archive_sequence`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9442790d8636fca14922ac264eb2de0f22ca08ea369ca5e4a30f8285015a393c -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'object', label: 'str' = 'archive sequence') -> 'int'\""
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "parse_archive_sequence",
  "unit": "export"
}
```

</details>
