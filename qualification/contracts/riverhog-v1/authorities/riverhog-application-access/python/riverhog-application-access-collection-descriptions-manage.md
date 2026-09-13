# riverhog_application_access.COLLECTION_DESCRIPTIONS_MANAGE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-collection-de-8be1b7c40a:9caf2a277c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-48c6899b9f"></a>
| Field | Shape |
|---|---|
| <a id="s-e94c8ca790"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-ac637a56c3"></a>`distribution` | "riverhog-application-access" |
| <a id="s-2fb40fe06a"></a>`module` | "riverhog_application_access" |
| <a id="s-a0bf7d82b4"></a>`name` | "COLLECTION_DESCRIPTIONS_MANAGE" |
| <a id="s-1941f0574c"></a>`unit` | "export" |

## Governing policies

- <a id="pa-4fc01aa152"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources.md#src-9d9ce5fdac) — `packages/riverhog-application-access/src/riverhog_application_access/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_application_access.COLLECTION_DESCRIPTIONS_MANAGE`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8baf7ee8865388344bc46efbdfd75771e45827cf72511fcf0ea28d46f8ea98d1 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "collection-descriptions:manage"
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "COLLECTION_DESCRIPTIONS_MANAGE",
  "unit": "export"
}
```
