# gogurt_core.GogurtProviderReference

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-core:gogurt-core-gogurtproviderreference:f94422b2ad -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f46a8ec3f6"></a>
- <a id="s-41445ab8af"></a>`distribution`: `gogurt-core`
- <a id="s-e19f849445"></a>`module`: `gogurt_core`
- <a id="s-bc04d9e672"></a>`name`: `GogurtProviderReference`
- <a id="s-cc77f665d8"></a>`unit`: `export`

### Declared structure

- <a id="s-9d96804bb7"></a>`kind`: `"class"`
- <a id="s-c1f563fee2"></a>`signature`: `"\"(kind: 'GogurtProviderKind', name: 'str', provider_id: 'str', format: 'str' = 'gogurt-provider-reference/v1') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-b5c754032b"></a>`kind` | `'GogurtProviderKind'` | `required` |
| <a id="s-b7a63935f9"></a>`name` | `'str'` | `required` |
| <a id="s-e99960a627"></a>`provider_id` | `'str'` | `required` |
| <a id="s-b7a9e18096"></a>`format` | `'str'` | `'gogurt-provider-reference/v1'` |

## Maintained corroboration

### Related interface records

- [as_dict](gogurt-core-gogurtproviderreference-as-dict.md)
- [from_mapping](gogurt-core-gogurtproviderreference-from-mapping.md)

## Governing policies

- <a id="pa-8e521a08f3"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-core:gogurt_core](../../../evidence/sources.md#src-e253e4a684) — `reference/gogurt/packages/core/src/gogurt_core/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_core.GogurtProviderReference`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 97b4b308816aa0100328cc2950057510eb73670c73e6af9ea1c3efbc677b8081 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "kind",
        "type": "'GogurtProviderKind'"
      },
      {
        "default": "required",
        "name": "name",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "provider_id",
        "type": "'str'"
      },
      {
        "default": "'gogurt-provider-reference/v1'",
        "name": "format",
        "type": "'str'"
      }
    ],
    "kind": "class",
    "signature": "\"(kind: 'GogurtProviderKind', name: 'str', provider_id: 'str', format: 'str' = 'gogurt-provider-reference/v1') -> None\""
  },
  "distribution": "gogurt-core",
  "module": "gogurt_core",
  "name": "GogurtProviderReference",
  "unit": "export"
}
```

</details>
