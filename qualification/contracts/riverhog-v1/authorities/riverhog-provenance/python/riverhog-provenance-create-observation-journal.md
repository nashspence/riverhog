# riverhog_provenance.create_observation_journal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-create-observation-journal:b513108ca0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-679a5c3c62"></a>
- <a id="s-32470d9f8e"></a>`distribution`: `riverhog-provenance`
- <a id="s-de9e1f3327"></a>`module`: `riverhog_provenance`
- <a id="s-77a0d6f249"></a>`name`: `create_observation_journal`
- <a id="s-b6bea50b47"></a>`unit`: `export`

### Declared structure

- <a id="s-6a6167ff53"></a>`kind`: `"function"`
- <a id="s-1fe8275606"></a>`signature`: `"\"(path: 'Path', *, relative_path: 'str', host_id: 'str', agent_name: 'str', agent_version: 'str', observer: 'FileStateObserver', policy: 'ObservationPolicy \| None' = None) -> 'bytes'\""`

## Governing policies

- <a id="pa-db40b5e4f9"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources/authorities.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.create_observation_journal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8486f768a21728cf53d6768819dab44075cf10d2ee9dcea09d5ab6d7754b6050 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(path: 'Path', *, relative_path: 'str', host_id: 'str', agent_name: 'str', agent_version: 'str', observer: 'FileStateObserver', policy: 'ObservationPolicy | None' = None) -> 'bytes'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "create_observation_journal",
  "unit": "export"
}
```

</details>
