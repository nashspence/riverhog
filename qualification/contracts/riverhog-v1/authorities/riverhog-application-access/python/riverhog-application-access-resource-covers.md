# riverhog_application_access.resource_covers

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-resource-covers:04699806de -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e39990bda6"></a>
- <a id="s-2a78a826b8"></a>`distribution`: `riverhog-application-access`
- <a id="s-6e27cbf6a8"></a>`module`: `riverhog_application_access`
- <a id="s-59a7c3ce6c"></a>`name`: `resource_covers`
- <a id="s-c999a5cf3e"></a>`unit`: `export`

### Declared structure

- <a id="s-dcca481ee2"></a>`kind`: `"function"`
- <a id="s-5e79ada953"></a>`signature`: `"\"(grantor: 'str', requested: 'str') -> 'bool'\""`

## Governing policies

- <a id="pa-9514dcebe8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources.md#src-9d9ce5fdac) — `packages/riverhog-application-access/src/riverhog_application_access/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_application_access.resource_covers`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4559e3ea7206eb4855268f5822502017b0138e435a7f567d302c0ec8f227d26d -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(grantor: 'str', requested: 'str') -> 'bool'\""
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "resource_covers",
  "unit": "export"
}
```
