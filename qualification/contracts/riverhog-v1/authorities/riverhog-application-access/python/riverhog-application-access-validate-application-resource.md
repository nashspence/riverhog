# riverhog_application_access.validate_application_resource

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-validate-appl-832e8fa99d:9d73961e88 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9a9cf2c6e0"></a>
- <a id="s-2d735fab42"></a>`distribution`: `riverhog-application-access`
- <a id="s-f4d1eb7fc5"></a>`module`: `riverhog_application_access`
- <a id="s-6d22087cf1"></a>`name`: `validate_application_resource`
- <a id="s-0db237f7fe"></a>`unit`: `export`

### Declared structure

- <a id="s-a994df2bc6"></a>`kind`: `"function"`
- <a id="s-c88ddce533"></a>`signature`: `"\"(value: 'str') -> 'str'\""`

## Governing policies

- <a id="pa-a582780ad7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources.md#src-9d9ce5fdac) — `packages/riverhog-application-access/src/riverhog_application_access/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_application_access.validate_application_resource`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f88f9f974ecd3d7b38bb6295c5479e2606afc01bd3674fb8d40293d1eb5d4177 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'str') -> 'str'\""
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "validate_application_resource",
  "unit": "export"
}
```
