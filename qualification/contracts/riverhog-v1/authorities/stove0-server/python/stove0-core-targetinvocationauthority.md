# stove0_core.TargetInvocationAuthority

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-targetinvocationauthority:bfc0707647 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ff0f2b9c06"></a>
- <a id="s-861887ced6"></a>`distribution`: `stove0-server`
- <a id="s-8a33b7a78f"></a>`module`: `stove0_core`
- <a id="s-b4cbefde73"></a>`name`: `TargetInvocationAuthority`
- <a id="s-17bb16cbfd"></a>`unit`: `export`

### Declared structure

- <a id="s-0b9a0cec77"></a>`kind`: `"class"`
- <a id="s-4302ea9d23"></a>`signature`: `"\"(runtime: 'TargetRuntimeAuthority', workspace_assurance: 'WorkspaceAssurance') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-2ef66a97b7"></a>`runtime` | `'TargetRuntimeAuthority'` | `required` |
| <a id="s-279d26143b"></a>`workspace_assurance` | `'WorkspaceAssurance'` | `required` |

## Governing policies

- <a id="pa-802d2fdd67"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.TargetInvocationAuthority`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cf63f20ecfa8a84ea6aab33c628b93cf7ac1788e0e939766f709fc527b2bac6c -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "runtime",
        "type": "'TargetRuntimeAuthority'"
      },
      {
        "default": "required",
        "name": "workspace_assurance",
        "type": "'WorkspaceAssurance'"
      }
    ],
    "kind": "class",
    "signature": "\"(runtime: 'TargetRuntimeAuthority', workspace_assurance: 'WorkspaceAssurance') -> None\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "TargetInvocationAuthority",
  "unit": "export"
}
```

</details>
