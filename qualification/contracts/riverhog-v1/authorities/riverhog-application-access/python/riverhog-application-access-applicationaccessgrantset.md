# riverhog_application_access.ApplicationAccessGrantSet

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-applicationac-d1ab2b2bec:6297aef642 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-20a2359614"></a>
| Field | Shape |
|---|---|
| <a id="s-aa2dee159a"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-356f252e2b"></a>`distribution` | "riverhog-application-access" |
| <a id="s-52e1c566f6"></a>`module` | "riverhog_application_access" |
| <a id="s-3b142ea758"></a>`name` | "ApplicationAccessGrantSet" |
| <a id="s-0d1940f3a2"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_application_access.ApplicationAccessGrantSet.validate_set](riverhog-application-access-applicationaccessgrantset-validate-set.md)

## Governing policies

- <a id="pa-c98301a488"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources.md#src-9d9ce5fdac) — `packages/riverhog-application-access/src/riverhog_application_access/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_application_access.ApplicationAccessGrantSet`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dbcdcc8f530b760715f6e8348067423e120d9c9e8d17424b2e3fbe62b9bb6d98 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "d10037ab0ccafe138959e093e3b62aeac69ae2b01271ebeabc7550a9ecc98897",
    "signature": "\"(root: 'RootModelRootType' = PydanticUndefined) -> None\""
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "ApplicationAccessGrantSet",
  "unit": "export"
}
```
