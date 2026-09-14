# stove0_protocol.canonical_json_sha256

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-canonical-json-sha256:4b4c7f9e01 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-db512c3f58"></a>
- <a id="s-e9895fb108"></a>`distribution`: `stove0-protocol`
- <a id="s-1d883f1a73"></a>`module`: `stove0_protocol`
- <a id="s-75e10c9ecf"></a>`name`: `canonical_json_sha256`
- <a id="s-4f14e4c932"></a>`unit`: `export`

### Declared structure

- <a id="s-a57a65adce"></a>`kind`: `"function"`
- <a id="s-b58ae6a0c3"></a>`signature`: `"\"(value: 'object') -> 'str'\""`

## Governing policies

- <a id="pa-84caa8b8b9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.canonical_json_sha256`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 14554abea62e15646aafb6ff69a0ea63df4bf626bb5fbeb6aaa4b87063863fed -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'object') -> 'str'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "canonical_json_sha256",
  "unit": "export"
}
```
