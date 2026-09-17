# riverhog_provenance.canonical_sidecar_path

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-canonical-sidecar-path:6d0538da51 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-512eb80fc1"></a>
- <a id="s-f4ca3f1aae"></a>`distribution`: `riverhog-provenance`
- <a id="s-18410f45a7"></a>`module`: `riverhog_provenance`
- <a id="s-e0b79ce8fa"></a>`name`: `canonical_sidecar_path`
- <a id="s-078635960c"></a>`unit`: `export`

### Declared structure

- <a id="s-b64a2e1466"></a>`kind`: `"function"`
- <a id="s-679c54b379"></a>`signature`: `"\"(payload: 'Path') -> 'Path'\""`

## Governing policies

- <a id="pa-4ffef920fc"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.canonical_sidecar_path`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b7e82ee97c62ebdff169ead58ec7fa2b2bad43184a78e84c6f187d48d816ef87 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(payload: 'Path') -> 'Path'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "canonical_sidecar_path",
  "unit": "export"
}
```

</details>
