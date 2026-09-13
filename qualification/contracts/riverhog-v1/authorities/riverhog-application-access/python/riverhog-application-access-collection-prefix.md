# riverhog_application_access.COLLECTION_PREFIX

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-collection-prefix:492ea2dc5f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-91ddeb32ce"></a>
| Field | Shape |
|---|---|
| <a id="s-cdd1e1feee"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-063d56efe8"></a>`distribution` | "riverhog-application-access" |
| <a id="s-6bdb9a1e8f"></a>`module` | "riverhog_application_access" |
| <a id="s-b0c33c0aa8"></a>`name` | "COLLECTION_PREFIX" |
| <a id="s-8cccf12a6d"></a>`unit` | "export" |

## Governing policies

- <a id="pa-3a7d8c523e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources.md#src-9d9ce5fdac) — `packages/riverhog-application-access/src/riverhog_application_access/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_application_access.COLLECTION_PREFIX`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b5fc4aafe58d9e2c5fd20708c28dc903e554433dbdf859338388b41783fff40d -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "collection:"
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "COLLECTION_PREFIX",
  "unit": "export"
}
```
