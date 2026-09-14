# riverhog_protocol.validate_collection_description

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-validate-collection-description:bb61264ba5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bbcf6a82ed"></a>
- <a id="s-1fe326e775"></a>`distribution`: `riverhog-protocol`
- <a id="s-5bcff4fb22"></a>`module`: `riverhog_protocol`
- <a id="s-18bfbf4e9f"></a>`name`: `validate_collection_description`
- <a id="s-96fc422e49"></a>`unit`: `export`

### Declared structure

- <a id="s-5bb1185645"></a>`kind`: `"function"`
- <a id="s-b546ef0f5a"></a>`signature`: `"\"(value: 'str') -> 'str'\""`

## Governing policies

- <a id="pa-33e5099cbb"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.validate_collection_description`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b867163f38681cc0e33d3cb62dd7a0b405d3d831d10c5a95869c14444489a33c -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'str') -> 'str'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "validate_collection_description",
  "unit": "export"
}
```
