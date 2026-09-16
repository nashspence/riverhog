# stove0_protocol.canonical_json_bytes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-canonical-json-bytes:541189e6b9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a55ed78bb9"></a>
- <a id="s-ead3537f7a"></a>`distribution`: `stove0-protocol`
- <a id="s-d36dd67f47"></a>`module`: `stove0_protocol`
- <a id="s-e2f5d2462f"></a>`name`: `canonical_json_bytes`
- <a id="s-f77bb64261"></a>`unit`: `export`

### Declared structure

- <a id="s-78a287a070"></a>`kind`: `"function"`
- <a id="s-a1ba2bcdd4"></a>`signature`: `"\"(value: 'object') -> 'bytes'\""`

## Governing policies

- <a id="pa-c84f955a84"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.canonical_json_bytes`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 84278e310317b369c1857698be4abb816aab9279c36e6f875df68138bfb0de4f -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'object') -> 'bytes'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "canonical_json_bytes",
  "unit": "export"
}
```

</details>
