# riverhog_application_access.ApplicationAccessGrant.validate_relationship

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-applicationac-eb6971df01:8e25df94ba -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9edf62fc8b"></a>
| Field | Shape |
|---|---|
| <a id="s-c9b9b8b38f"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-bbf97e1511"></a>`distribution` | "riverhog-application-access" |
| <a id="s-60f8b335a9"></a>`module` | "riverhog_application_access" |
| <a id="s-d2c13c4722"></a>`name` | "validate_relationship" |
| <a id="s-62cedad657"></a>`owner` | "riverhog_application_access.ApplicationAccessGrant" |
| <a id="s-fe06532377"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_application_access.ApplicationAccessGrant](riverhog-application-access-applicationaccessgrant.md)

## Governing policies

- <a id="pa-141356210c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources.md#src-9d9ce5fdac) — `packages/riverhog-application-access/src/riverhog_application_access/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_application_access.ApplicationAccessGrant.validate_relationship`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 075971d91d94835a4868291fe3c28fd40b5bdca6682314b09f4580436f771303 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'ApplicationAccessGrant'\""
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "validate_relationship",
  "owner": "riverhog_application_access.ApplicationAccessGrant",
  "unit": "member"
}
```
