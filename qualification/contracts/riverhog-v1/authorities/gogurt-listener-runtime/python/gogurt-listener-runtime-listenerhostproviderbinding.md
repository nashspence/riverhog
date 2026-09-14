# gogurt_listener_runtime.ListenerHostProviderBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-listenerhostproviderbinding:44e13b6919 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2ddd4bda91"></a>
- <a id="s-ab2faf60bd"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-c419ecb23c"></a>`module`: `gogurt_listener_runtime`
- <a id="s-6d7b11d05b"></a>`name`: `ListenerHostProviderBinding`
- <a id="s-391d2842ea"></a>`unit`: `export`

### Declared structure

- <a id="s-309d7ad08c"></a>`kind`: `"class"`
- <a id="s-4530f228ec"></a>`signature`: `"\"(provider_id: 'str', paths: 'Callable[[], ListenerRuntimePaths]', adapter: 'Callable[[], ListenerAdapter]', executable: 'Callable[[str \| None], Path]', format: 'str' = 'gogurt-listener-host-provider-binding/v1') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-ba394e496c"></a>`provider_id` | `'str'` | `required` |
| <a id="s-0eed4dba04"></a>`paths` | `'Callable[[], ListenerRuntimePaths]'` | `required` |
| <a id="s-8804dc955a"></a>`adapter` | `'Callable[[], ListenerAdapter]'` | `required` |
| <a id="s-2871a9eea8"></a>`executable` | `'Callable[[str \| None], Path]'` | `required` |
| <a id="s-96b692e585"></a>`format` | `'str'` | `'gogurt-listener-host-provider-binding/v1'` |

## Governing policies

- <a id="pa-6ca43b92b5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources.md#src-259980dd25) — `reference/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.ListenerHostProviderBinding`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c775b0a82e4675c7a7b9896c90c628b8a5ae82dc94c0271dc2d80de9acdee180 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "provider_id",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "paths",
        "type": "'Callable[[], ListenerRuntimePaths]'"
      },
      {
        "default": "required",
        "name": "adapter",
        "type": "'Callable[[], ListenerAdapter]'"
      },
      {
        "default": "required",
        "name": "executable",
        "type": "'Callable[[str | None], Path]'"
      },
      {
        "default": "'gogurt-listener-host-provider-binding/v1'",
        "name": "format",
        "type": "'str'"
      }
    ],
    "kind": "class",
    "signature": "\"(provider_id: 'str', paths: 'Callable[[], ListenerRuntimePaths]', adapter: 'Callable[[], ListenerAdapter]', executable: 'Callable[[str | None], Path]', format: 'str' = 'gogurt-listener-host-provider-binding/v1') -> None\""
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "ListenerHostProviderBinding",
  "unit": "export"
}
```
