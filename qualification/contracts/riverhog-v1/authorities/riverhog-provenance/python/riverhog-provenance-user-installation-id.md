# riverhog_provenance.user_installation_id

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-user-installation-id:e8730ad17e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f7b89b5356"></a>
- <a id="s-2a191e4716"></a>`distribution`: `riverhog-provenance`
- <a id="s-eb4ee3bc89"></a>`module`: `riverhog_provenance`
- <a id="s-06f2009398"></a>`name`: `user_installation_id`
- <a id="s-4ac1436750"></a>`unit`: `export`

### Declared structure

- <a id="s-f7712e8485"></a>`kind`: `"function"`
- <a id="s-ac42b247c3"></a>`signature`: `"\"(application: 'str') -> 'str'\""`

## Governing policies

- <a id="pa-7f475f54dd"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.user_installation_id`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c2f3b3c22f2046de34e181628df59f5c32c4a4a0fd645eb44f6be4c3a4fda1b2 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(application: 'str') -> 'str'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "user_installation_id",
  "unit": "export"
}
```

</details>
