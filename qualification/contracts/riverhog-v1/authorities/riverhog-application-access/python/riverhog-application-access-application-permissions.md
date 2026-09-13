# riverhog_application_access.APPLICATION_PERMISSIONS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-application-permissions:004992de49 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2783287f06"></a>
| Field | Shape |
|---|---|
| <a id="s-2b5a6cb9ac"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-4f9a991362"></a>`distribution` | "riverhog-application-access" |
| <a id="s-5a0c73e726"></a>`module` | "riverhog_application_access" |
| <a id="s-211a002942"></a>`name` | "APPLICATION_PERMISSIONS" |
| <a id="s-2a07b36096"></a>`unit` | "export" |

## Governing policies

- <a id="pa-f4024a05fa"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources.md#src-9d9ce5fdac) — `packages/riverhog-application-access/src/riverhog_application_access/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_application_access.APPLICATION_PERMISSIONS`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b81857b330b310f1923bb10a053f983c5163f7103464c210e0ae14b75d4a9ba4 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": [
      "archives:manage",
      "archives:read",
      "catalog:read",
      "collection-descriptions:manage",
      "collection-tags:manage",
      "collection-transforms:control",
      "collection-transforms:execute",
      "collections:create",
      "collections:delete",
      "events:read",
      "events:read_all",
      "keys:manage",
      "provenance:export",
      "provenance:read",
      "quotas:manage",
      "retrieval:manage"
    ]
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "APPLICATION_PERMISSIONS",
  "unit": "export"
}
```
