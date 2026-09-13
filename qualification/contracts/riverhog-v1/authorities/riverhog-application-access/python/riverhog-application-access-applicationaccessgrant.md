# riverhog_application_access.ApplicationAccessGrant

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-applicationaccessgrant:fbd0a6fd2d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-487340b284"></a>
| Field | Shape |
|---|---|
| <a id="s-6653322379"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-e4911eddea"></a>`distribution` | "riverhog-application-access" |
| <a id="s-2403018936"></a>`module` | "riverhog_application_access" |
| <a id="s-f25a65db4c"></a>`name` | "ApplicationAccessGrant" |
| <a id="s-e84cd86f89"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_application_access.ApplicationAccessGrant.as_access](riverhog-application-access-applicationaccessgrant-as-access.md)
- [riverhog_application_access.ApplicationAccessGrant.validate_relationship](riverhog-application-access-applicationaccessgrant-validate-relationship.md)

## Governing policies

- <a id="pa-48133bba5a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources.md#src-9d9ce5fdac) — `packages/riverhog-application-access/src/riverhog_application_access/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_application_access.ApplicationAccessGrant`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 34e429094469cb8fcaf1ff91b6a3165142901b22d547e51fcb0a62b2e58e2a3f -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "a587a8ba6e6662132a0d755eaea10095b5ab546cf17c3e2bb525d93630fb90e1",
    "signature": "\"(*, permission: ApplicationPermission, resource: ApplicationResource = '*') -> None\""
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "ApplicationAccessGrant",
  "unit": "export"
}
```
