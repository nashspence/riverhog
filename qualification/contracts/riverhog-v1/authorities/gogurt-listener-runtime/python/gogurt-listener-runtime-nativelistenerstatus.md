# gogurt_listener_runtime.NativeListenerStatus

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-nativelistenerstatus:7b51220235 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a1e432c91d"></a>
| Field | Shape |
|---|---|
| <a id="s-8430b4e23f"></a>`contract` | additional keys=`fields`, `kind`, `signature` |
| <a id="s-c2d6878f1a"></a>`distribution` | "gogurt-listener-runtime" |
| <a id="s-4f252a60f6"></a>`module` | "gogurt_listener_runtime" |
| <a id="s-070575567d"></a>`name` | "NativeListenerStatus" |
| <a id="s-86140ff676"></a>`unit` | "export" |

## Governing policies

- <a id="pa-209f4ebe2d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources.md#src-259980dd25) — `reference/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.NativeListenerStatus`

### Exact owned JSON

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
