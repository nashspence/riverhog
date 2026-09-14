# stove0_target_protocol.canonical_json_sha256

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-canonical-json-sha256:b65112eb5e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6053f9f93f"></a>
- <a id="s-b11bbfbcae"></a>`distribution`: `stove0-target-protocol`
- <a id="s-3cd5240746"></a>`module`: `stove0_target_protocol`
- <a id="s-6b4d52f04a"></a>`name`: `canonical_json_sha256`
- <a id="s-8ae02a6780"></a>`unit`: `export`

### Declared structure

- <a id="s-67edca751e"></a>`kind`: `"function"`
- <a id="s-e52817eb47"></a>`signature`: `"\"(value: 'object') -> 'str'\""`

## Governing policies

- <a id="pa-770380e888"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.canonical_json_sha256`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 601da8b5acc1aa6cf486d6c92fd064d3f4e281e71e993c5ac8c0e4a442e5c7c5 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'object') -> 'str'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "canonical_json_sha256",
  "unit": "export"
}
```
