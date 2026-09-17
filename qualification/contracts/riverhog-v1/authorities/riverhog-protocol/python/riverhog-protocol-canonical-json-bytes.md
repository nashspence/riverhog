# riverhog_protocol.canonical_json_bytes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-canonical-json-bytes:f422fb0006 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a1192d6ee7"></a>
- <a id="s-b867905977"></a>`distribution`: `riverhog-protocol`
- <a id="s-3b4f3deb95"></a>`module`: `riverhog_protocol`
- <a id="s-9e26aaaab8"></a>`name`: `canonical_json_bytes`
- <a id="s-0558ed0b25"></a>`unit`: `export`

### Declared structure

- <a id="s-5706c9a1dd"></a>`kind`: `"function"`
- <a id="s-070018078a"></a>`signature`: `"\"(value: 'object') -> 'bytes'\""`

## Governing policies

- <a id="pa-f0895a8c24"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.canonical_json_bytes`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3a9cfd9c49f6b2f98e4e2eb747835e8ee5b9b806e262139c6be453b270b149fd -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'object') -> 'bytes'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "canonical_json_bytes",
  "unit": "export"
}
```

</details>
