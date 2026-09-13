# riverhog_application_access.ApplicationAccessGrant.as_access

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-applicationac-7f3711ec69:1539010bd7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ad9f2e55de"></a>
| Field | Shape |
|---|---|
| <a id="s-c6638134d1"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-0367037543"></a>`distribution` | "riverhog-application-access" |
| <a id="s-4c41186cb3"></a>`module` | "riverhog_application_access" |
| <a id="s-fcc7fb575c"></a>`name` | "as_access" |
| <a id="s-6a6804902e"></a>`owner` | "riverhog_application_access.ApplicationAccessGrant" |
| <a id="s-ab2b3a35ca"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_application_access.ApplicationAccessGrant](riverhog-application-access-applicationaccessgrant.md)

## Governing policies

- <a id="pa-42c420d4d1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources.md#src-9d9ce5fdac) — `packages/riverhog-application-access/src/riverhog_application_access/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_application_access.ApplicationAccessGrant.as_access`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 22f60e898e27cfadb833b6bdccd945c8934577dd29b279c0d4f4978aa0bc8630 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'ApplicationAccess'\""
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "as_access",
  "owner": "riverhog_application_access.ApplicationAccessGrant",
  "unit": "member"
}
```
