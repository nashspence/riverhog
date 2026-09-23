# gogurt_listener_runtime.ListenerConfig

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-listenerconfig:bf5adfbb7c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-070199c165"></a>
- <a id="s-10d2b301bf"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-85a9f64738"></a>`module`: `gogurt_listener_runtime`
- <a id="s-0ecbbe7fe0"></a>`name`: `ListenerConfig`
- <a id="s-a6329e81f8"></a>`unit`: `export`

### Declared structure

- <a id="s-574cbf7166"></a>`kind`: `"class"`
- <a id="s-4e065b5a9e"></a>`signature`: `"\"(executable: 'Path', routes_file: 'Path', actions_dir: 'Path \| None', interval_seconds: 'float', state_dir: 'Path', mounted_volume_provider: 'GogurtProviderReference', listener_host_provider: 'GogurtProviderReference', autorun: 'bool' = True) -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-36da8e85a8"></a>`executable` | `'Path'` | `required` |
| <a id="s-d97b186294"></a>`routes_file` | `'Path'` | `required` |
| <a id="s-fb4f89c7c2"></a>`actions_dir` | `'Path \| None'` | `required` |
| <a id="s-f1201f84b8"></a>`interval_seconds` | `'float'` | `required` |
| <a id="s-0adb7c9f3b"></a>`state_dir` | `'Path'` | `required` |
| <a id="s-e9623d2e7d"></a>`mounted_volume_provider` | `'GogurtProviderReference'` | `required` |
| <a id="s-2068fcc7b3"></a>`listener_host_provider` | `'GogurtProviderReference'` | `required` |
| <a id="s-ac3dc937a6"></a>`autorun` | `'bool'` | `True` |

## Maintained corroboration

### Related interface records

- [content](gogurt-listener-runtime-listenerconfig-content.md)
- [payload](gogurt-listener-runtime-listenerconfig-payload.md)
- [read](gogurt-listener-runtime-listenerconfig-read.md)
- [write](gogurt-listener-runtime-listenerconfig-write.md)

## Governing policies

- <a id="pa-6373f140d4"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources/authorities.md#src-259980dd25) — [some-implementations/gogurt/packages/listener-runtime/src/gogurt\_listener\_runtime/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.ListenerConfig`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 238d15b4d6f7ef1432c953203efefcdcaf21234ee0550f208eeef9013d9e9b1f -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "executable",
        "type": "'Path'"
      },
      {
        "default": "required",
        "name": "routes_file",
        "type": "'Path'"
      },
      {
        "default": "required",
        "name": "actions_dir",
        "type": "'Path | None'"
      },
      {
        "default": "required",
        "name": "interval_seconds",
        "type": "'float'"
      },
      {
        "default": "required",
        "name": "state_dir",
        "type": "'Path'"
      },
      {
        "default": "required",
        "name": "mounted_volume_provider",
        "type": "'GogurtProviderReference'"
      },
      {
        "default": "required",
        "name": "listener_host_provider",
        "type": "'GogurtProviderReference'"
      },
      {
        "default": "True",
        "name": "autorun",
        "type": "'bool'"
      }
    ],
    "kind": "class",
    "signature": "\"(executable: 'Path', routes_file: 'Path', actions_dir: 'Path | None', interval_seconds: 'float', state_dir: 'Path', mounted_volume_provider: 'GogurtProviderReference', listener_host_provider: 'GogurtProviderReference', autorun: 'bool' = True) -> None\""
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "ListenerConfig",
  "unit": "export"
}
```

</details>
