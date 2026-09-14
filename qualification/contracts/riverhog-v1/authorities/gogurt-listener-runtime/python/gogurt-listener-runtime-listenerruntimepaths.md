# gogurt_listener_runtime.ListenerRuntimePaths

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-listenerruntimepaths:5a7253d270 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d33d6037e6"></a>
- <a id="s-3eec109d1d"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-d3d7fe3923"></a>`module`: `gogurt_listener_runtime`
- <a id="s-5ff2d04b5a"></a>`name`: `ListenerRuntimePaths`
- <a id="s-be1ae60a41"></a>`unit`: `export`

### Declared structure

- <a id="s-4d2c826d77"></a>`kind`: `"class"`
- <a id="s-ef3880a2d5"></a>`signature`: `"\"(state_dir: 'Path', config_file: 'Path', database_file: 'Path', heartbeat_file: 'Path', lock_file: 'Path', log_file: 'Path', stop_file: 'Path') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-cd0256208d"></a>`state_dir` | `'Path'` | `required` |
| <a id="s-cfc4c6600a"></a>`config_file` | `'Path'` | `required` |
| <a id="s-c933935c4b"></a>`database_file` | `'Path'` | `required` |
| <a id="s-f44055412a"></a>`heartbeat_file` | `'Path'` | `required` |
| <a id="s-980de31586"></a>`lock_file` | `'Path'` | `required` |
| <a id="s-8b14738bbd"></a>`log_file` | `'Path'` | `required` |
| <a id="s-88fd9bbf7a"></a>`stop_file` | `'Path'` | `required` |

## Governing policies

- <a id="pa-2387a995f6"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources.md#src-259980dd25) — `reference/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.ListenerRuntimePaths`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b0b3df015d6b83733a578d68220137ee7db96252141c133428243c1f598e6bd9 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "state_dir",
        "type": "'Path'"
      },
      {
        "default": "required",
        "name": "config_file",
        "type": "'Path'"
      },
      {
        "default": "required",
        "name": "database_file",
        "type": "'Path'"
      },
      {
        "default": "required",
        "name": "heartbeat_file",
        "type": "'Path'"
      },
      {
        "default": "required",
        "name": "lock_file",
        "type": "'Path'"
      },
      {
        "default": "required",
        "name": "log_file",
        "type": "'Path'"
      },
      {
        "default": "required",
        "name": "stop_file",
        "type": "'Path'"
      }
    ],
    "kind": "class",
    "signature": "\"(state_dir: 'Path', config_file: 'Path', database_file: 'Path', heartbeat_file: 'Path', lock_file: 'Path', log_file: 'Path', stop_file: 'Path') -> None\""
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "ListenerRuntimePaths",
  "unit": "export"
}
```
