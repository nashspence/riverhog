# riverhog_application_access.ApplicationAccessGrantSet.validate_set

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-applicationac-a956c856fd:53e1bf5da6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a9dc66e331"></a>
- <a id="s-b6a10f6fd6"></a>`distribution`: `riverhog-application-access`
- <a id="s-af139ae376"></a>`module`: `riverhog_application_access`
- <a id="s-0f15215d37"></a>`name`: `validate_set`
- <a id="s-8397e991cb"></a>`owner`: `riverhog_application_access.ApplicationAccessGrantSet`
- <a id="s-dfcd04dd0c"></a>`unit`: `member`

### Declared structure

- <a id="s-628e293bb6"></a>`kind`: `"method"`
- <a id="s-9377db3056"></a>`signature`: `"\"(self) -> 'ApplicationAccessGrantSet'\""`

## Maintained corroboration

### Related interface records

- [ApplicationAccessGrantSet](riverhog-application-access-applicationaccessgrantset.md)

## Governing policies

- <a id="pa-09c8d4cf84"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources.md#src-9d9ce5fdac) — `packages/riverhog-application-access/src/riverhog_application_access/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_application_access.ApplicationAccessGrantSet.validate_set`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 13b18c337b2ed3efed3bf7ff0d81a39e90017ad0f9b0dd52b2956a369bb0dd4c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'ApplicationAccessGrantSet'\""
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "validate_set",
  "owner": "riverhog_application_access.ApplicationAccessGrantSet",
  "unit": "member"
}
```
