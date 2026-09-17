# gogurt_core.GogurtAction

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-core:gogurt-core-gogurtaction:58e9214bdc -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0ae304c2ba"></a>
- <a id="s-f43dfcda83"></a>`distribution`: `gogurt-core`
- <a id="s-8b1cd39b43"></a>`module`: `gogurt_core`
- <a id="s-5bb8fc9add"></a>`name`: `GogurtAction`
- <a id="s-4197c62cf4"></a>`unit`: `export`

### Declared structure

- <a id="s-4d72801466"></a>`kind`: `"class"`
- <a id="s-db90722750"></a>`signature`: `"\"(route: 'str', command: 'tuple[str, ...]') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-01e5441076"></a>`route` | `'str'` | `required` |
| <a id="s-2f730b91a7"></a>`command` | `'tuple[str, ...]'` | `required` |

## Governing policies

- <a id="pa-70e9c04c96"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-core:gogurt_core](../../../evidence/sources/authorities.md#src-e253e4a684) — [reference/gogurt/packages/core/src/gogurt\_core/\_\_init\_\_.py](../../../../../../reference/gogurt/packages/core/src/gogurt_core/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_core.GogurtAction`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 102bec3326dafbc458d803fa9ac61edd805fe9dac68a7852e9969cf915e33c73 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "route",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "command",
        "type": "'tuple[str, ...]'"
      }
    ],
    "kind": "class",
    "signature": "\"(route: 'str', command: 'tuple[str, ...]') -> None\""
  },
  "distribution": "gogurt-core",
  "module": "gogurt_core",
  "name": "GogurtAction",
  "unit": "export"
}
```

</details>
