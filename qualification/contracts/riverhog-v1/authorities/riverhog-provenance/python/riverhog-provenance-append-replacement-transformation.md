# riverhog_provenance.append_replacement_transformation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-append-replacement-tr-d1ce22b4be:d2085b9d02 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fe9f82eb95"></a>
- <a id="s-0c6d8a38d1"></a>`distribution`: `riverhog-provenance`
- <a id="s-04cdfe922d"></a>`module`: `riverhog_provenance`
- <a id="s-f87b73c2fc"></a>`name`: `append_replacement_transformation`
- <a id="s-6f30e6c4fb"></a>`unit`: `export`

### Declared structure

- <a id="s-6baa098047"></a>`kind`: `"function"`
- <a id="s-5d71714a27"></a>`signature`: `"\"(content: 'bytes', output_path: 'Path', *, relative_path: 'str', host_id: 'str', agent_name: 'str', agent_version: 'str', event_label: 'str', started_at: 'str', ended_at: 'str', observer: 'FileStateObserver', evidence: 'Sequence[Mapping[str, Any]]' = (), policy: 'ObservationPolicy \| None' = None) -> 'bytes'\""`

## Governing policies

- <a id="pa-f174f560f9"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources/authorities.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.append_replacement_transformation`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 56a529e0cff7ff2eaa080b4315fb6070d5cb5d410811fa9cddd9c40509f0e752 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(content: 'bytes', output_path: 'Path', *, relative_path: 'str', host_id: 'str', agent_name: 'str', agent_version: 'str', event_label: 'str', started_at: 'str', ended_at: 'str', observer: 'FileStateObserver', evidence: 'Sequence[Mapping[str, Any]]' = (), policy: 'ObservationPolicy | None' = None) -> 'bytes'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "append_replacement_transformation",
  "unit": "export"
}
```

</details>
