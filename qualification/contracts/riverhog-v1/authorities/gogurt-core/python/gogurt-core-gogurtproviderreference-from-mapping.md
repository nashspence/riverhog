# gogurt_core.GogurtProviderReference.from_mapping

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-core:gogurt-core-gogurtproviderreference-from-mapping:d67b5cdefd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bf619fe133"></a>
| Field | Shape |
|---|---|
| <a id="s-823fe7d70e"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-c1c61f92fc"></a>`distribution` | "gogurt-core" |
| <a id="s-064177b038"></a>`module` | "gogurt_core" |
| <a id="s-5ec4a64774"></a>`name` | "from_mapping" |
| <a id="s-839570100b"></a>`owner` | "gogurt_core.GogurtProviderReference" |
| <a id="s-08469ecc32"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [gogurt_core.GogurtProviderReference](gogurt-core-gogurtproviderreference.md)

## Governing policies

- <a id="pa-e8fd6216ba"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-core:gogurt_core](../../../evidence/sources.md#src-e253e4a684) — `reference/gogurt/packages/core/src/gogurt_core/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_core.GogurtProviderReference.from_mapping`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ba41c345b4b63afa92a949b69488052983139708324f0c4af2b9adc365d7ed95 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'object') -> 'GogurtProviderReference'\""
  },
  "distribution": "gogurt-core",
  "module": "gogurt_core",
  "name": "from_mapping",
  "owner": "gogurt_core.GogurtProviderReference",
  "unit": "member"
}
```
