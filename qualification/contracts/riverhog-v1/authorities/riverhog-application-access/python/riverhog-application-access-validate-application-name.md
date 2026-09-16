# riverhog_application_access.validate_application_name

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-validate-appl-7519c69b63:179d1cde09 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7a7b8d7fc8"></a>
- <a id="s-be7747f293"></a>`distribution`: `riverhog-application-access`
- <a id="s-bfc4f49333"></a>`module`: `riverhog_application_access`
- <a id="s-32e04f49b9"></a>`name`: `validate_application_name`
- <a id="s-6565348ea6"></a>`unit`: `export`

### Declared structure

- <a id="s-1200acd6aa"></a>`kind`: `"function"`
- <a id="s-74f9621f3c"></a>`signature`: `"\"(value: 'str') -> 'str'\""`

## Governing policies

- <a id="pa-979b391d80"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources.md#src-9d9ce5fdac) — `packages/riverhog-application-access/src/riverhog_application_access/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_application_access.validate_application_name`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9b04b62b08153065185c0b8b33e2f2e5f3d6d4c1c15c291123c3fdf2843f125c -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'str') -> 'str'\""
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "validate_application_name",
  "unit": "export"
}
```

</details>
