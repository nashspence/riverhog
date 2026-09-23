# gogurt_listener_runtime.NativeListenerStatus

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-nativelistenerstatus:7b51220235 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a1e432c91d"></a>
- <a id="s-c2d6878f1a"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-4f252a60f6"></a>`module`: `gogurt_listener_runtime`
- <a id="s-070575567d"></a>`name`: `NativeListenerStatus`
- <a id="s-86140ff676"></a>`unit`: `export`

### Declared structure

- <a id="s-c47886004f"></a>`kind`: `"class"`
- <a id="s-e9d52d8769"></a>`signature`: `"\"(installed: 'bool', enabled: 'bool', running: 'bool') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-525bc35ded"></a>`installed` | `'bool'` | `required` |
| <a id="s-9aceb0d064"></a>`enabled` | `'bool'` | `required` |
| <a id="s-112b7aef7b"></a>`running` | `'bool'` | `required` |

## Governing policies

- <a id="pa-209f4ebe2d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources/authorities.md#src-259980dd25) — [some-implementations/gogurt/packages/listener-runtime/src/gogurt\_listener\_runtime/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.NativeListenerStatus`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6bd623358bc30151598f8fe67c8a91d18710d5cc466698e880e7fe18ef5eeb84 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "installed",
        "type": "'bool'"
      },
      {
        "default": "required",
        "name": "enabled",
        "type": "'bool'"
      },
      {
        "default": "required",
        "name": "running",
        "type": "'bool'"
      }
    ],
    "kind": "class",
    "signature": "\"(installed: 'bool', enabled: 'bool', running: 'bool') -> None\""
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "NativeListenerStatus",
  "unit": "export"
}
```

</details>
