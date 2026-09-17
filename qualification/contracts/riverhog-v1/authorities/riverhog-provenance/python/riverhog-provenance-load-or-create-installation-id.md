# riverhog_provenance.load_or_create_installation_id

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-load-or-create-installation-id:22b2200c33 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2ee23352fb"></a>
- <a id="s-cb5684fa56"></a>`distribution`: `riverhog-provenance`
- <a id="s-68b096eb6c"></a>`module`: `riverhog_provenance`
- <a id="s-ddd3724fbe"></a>`name`: `load_or_create_installation_id`
- <a id="s-b2c1b04780"></a>`unit`: `export`

### Declared structure

- <a id="s-deca18a403"></a>`kind`: `"function"`
- <a id="s-500726852b"></a>`signature`: `"\"(path: 'Path') -> 'str'\""`

## Governing policies

- <a id="pa-fc884c1939"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.load_or_create_installation_id`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cb39dc71c530d8f7624b46acd5b1d618049b8f5dcac901ef301091ea610bd988 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(path: 'Path') -> 'str'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "load_or_create_installation_id",
  "unit": "export"
}
```

</details>
