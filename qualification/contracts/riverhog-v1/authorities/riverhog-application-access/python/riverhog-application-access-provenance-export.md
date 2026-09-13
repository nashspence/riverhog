# riverhog_application_access.PROVENANCE_EXPORT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-provenance-export:caa38a780b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bae9841a78"></a>
| Field | Shape |
|---|---|
| <a id="s-a36f07e9d2"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-7d923dfa01"></a>`distribution` | "riverhog-application-access" |
| <a id="s-dc9241260d"></a>`module` | "riverhog_application_access" |
| <a id="s-e7a4e99b81"></a>`name` | "PROVENANCE_EXPORT" |
| <a id="s-c8090ff33c"></a>`unit` | "export" |

## Governing policies

- <a id="pa-29f5d4a2ff"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources.md#src-9d9ce5fdac) — `packages/riverhog-application-access/src/riverhog_application_access/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_application_access.PROVENANCE_EXPORT`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e626500e5b5d053aa38ba79d3a31dd72391489b32d4c0a078cfb18959b6d71e1 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "provenance:export"
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "PROVENANCE_EXPORT",
  "unit": "export"
}
```
