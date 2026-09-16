# riverhog_provenance.append_observation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-append-observation:ac88022e7a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6adcedc66c"></a>
- <a id="s-2df69dcbb2"></a>`distribution`: `riverhog-provenance`
- <a id="s-d0ba5f92f8"></a>`module`: `riverhog_provenance`
- <a id="s-a86dd3a4ca"></a>`name`: `append_observation`
- <a id="s-ec9821ceb1"></a>`unit`: `export`

### Declared structure

- <a id="s-45879d99f1"></a>`kind`: `"function"`
- <a id="s-35a42fb6d9"></a>`signature`: `"\"(content: 'bytes', path: 'Path', *, relative_path: 'str', host_id: 'str', agent_name: 'str', agent_version: 'str', observer: 'FileStateObserver', policy: 'ObservationPolicy \| None' = None) -> 'bytes'\""`

## Governing policies

- <a id="pa-1876698fe4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.append_observation`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e94643e4d9acf5d1caf1dc784fa125b55dd5deeca61ce937ff715a2e4dd57cf2 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(content: 'bytes', path: 'Path', *, relative_path: 'str', host_id: 'str', agent_name: 'str', agent_version: 'str', observer: 'FileStateObserver', policy: 'ObservationPolicy | None' = None) -> 'bytes'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "append_observation",
  "unit": "export"
}
```

</details>
