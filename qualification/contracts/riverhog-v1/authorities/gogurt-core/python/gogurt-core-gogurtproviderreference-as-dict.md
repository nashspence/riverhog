# gogurt_core.GogurtProviderReference.as_dict

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-core:gogurt-core-gogurtproviderreference-as-dict:5472f90dcb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ac46a2c58b"></a>
- <a id="s-40b4ed2426"></a>`distribution`: `gogurt-core`
- <a id="s-ee3f628d4b"></a>`module`: `gogurt_core`
- <a id="s-3755b09311"></a>`name`: `as_dict`
- <a id="s-7c08ff4fb4"></a>`owner`: `gogurt_core.GogurtProviderReference`
- <a id="s-06c63adc7b"></a>`unit`: `member`

### Declared structure

- <a id="s-1a591676eb"></a>`kind`: `"method"`
- <a id="s-a97fb19890"></a>`signature`: `"\"(self) -> 'dict[str, str]'\""`

## Maintained corroboration

### Related interface records

- [GogurtProviderReference](gogurt-core-gogurtproviderreference.md)

## Governing policies

- <a id="pa-6a1e761a1e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-core:gogurt_core](../../../evidence/sources.md#src-e253e4a684) — `reference/gogurt/packages/core/src/gogurt_core/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_core.GogurtProviderReference.as_dict`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8f8e27d52d9a21f26cb8984289ab905e09516435590b2d6606fd576780b1536c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'dict[str, str]'\""
  },
  "distribution": "gogurt-core",
  "module": "gogurt_core",
  "name": "as_dict",
  "owner": "gogurt_core.GogurtProviderReference",
  "unit": "member"
}
```
