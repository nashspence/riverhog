# riverhog_application_access.validate_monthly_download_quota_bytes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-validate-mont-3e4934b0b6:9d31187aec -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5d6bf89011"></a>
- <a id="s-6ec32a7eba"></a>`distribution`: `riverhog-application-access`
- <a id="s-33cae25fd9"></a>`module`: `riverhog_application_access`
- <a id="s-0a18dd09fe"></a>`name`: `validate_monthly_download_quota_bytes`
- <a id="s-19d44899d0"></a>`unit`: `export`

### Declared structure

- <a id="s-8d58765c9a"></a>`kind`: `"function"`
- <a id="s-ce2dfeec50"></a>`signature`: `"\"(value: 'object') -> 'int'\""`

## Governing policies

- <a id="pa-4ac2564d95"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources.md#src-9d9ce5fdac) — `packages/riverhog-application-access/src/riverhog_application_access/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_application_access.validate_monthly_download_quota_bytes`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0f88a2811484b6c7fdddfd50bb62f652e20db6fd0be8ad3afa6a4c7ccf0e7434 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'object') -> 'int'\""
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "validate_monthly_download_quota_bytes",
  "unit": "export"
}
```
