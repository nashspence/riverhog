# riverhog_application_access.ARCHIVES_READ

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-archives-read:d198797190 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-affd8b41d2"></a>
| Field | Shape |
|---|---|
| <a id="s-72cf404176"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-9b33686e50"></a>`distribution` | "riverhog-application-access" |
| <a id="s-f0b46b8391"></a>`module` | "riverhog_application_access" |
| <a id="s-8b824c3238"></a>`name` | "ARCHIVES_READ" |
| <a id="s-7eb04960ee"></a>`unit` | "export" |

## Governing policies

- <a id="pa-0f9ca31334"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources.md#src-9d9ce5fdac) — `packages/riverhog-application-access/src/riverhog_application_access/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_application_access.ARCHIVES_READ`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8627ea3101f33014743e4a55b4e81d017f50c1e2ee48761412f356a5ace8d053 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "archives:read"
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "ARCHIVES_READ",
  "unit": "export"
}
```
