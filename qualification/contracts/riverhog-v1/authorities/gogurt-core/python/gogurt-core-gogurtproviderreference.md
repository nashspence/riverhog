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
| Field | Shape |
|---|---|
| <a id="s-b10dfcd6f7"></a>`contract` | additional keys=`fields`, `kind`, `signature` |
| <a id="s-41445ab8af"></a>`distribution` | "gogurt-core" |
| <a id="s-e19f849445"></a>`module` | "gogurt_core" |
| <a id="s-bc04d9e672"></a>`name` | "GogurtProviderReference" |
| <a id="s-cc77f665d8"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [gogurt_core.GogurtProviderReference.as_dict](gogurt-core-gogurtproviderreference-as-dict.md)
- [gogurt_core.GogurtProviderReference.from_mapping](gogurt-core-gogurtproviderreference-from-mapping.md)

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
