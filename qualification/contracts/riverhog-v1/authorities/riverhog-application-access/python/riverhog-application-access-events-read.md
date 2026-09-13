# riverhog_application_access.EVENTS_READ

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-events-read:9d73476532 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-74adffadf7"></a>
| Field | Shape |
|---|---|
| <a id="s-dde291745c"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-bd4a2e8f8c"></a>`distribution` | "riverhog-application-access" |
| <a id="s-14a6bf0296"></a>`module` | "riverhog_application_access" |
| <a id="s-74dfcb379d"></a>`name` | "EVENTS_READ" |
| <a id="s-742c8d81e0"></a>`unit` | "export" |

## Governing policies

- <a id="pa-242d9d91f6"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources.md#src-9d9ce5fdac) — `packages/riverhog-application-access/src/riverhog_application_access/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_application_access.EVENTS_READ`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fdf48dc60ae56760d48bae5e50122aeed20f4272e00b0773025e4018a9a6d2f8 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "events:read"
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "EVENTS_READ",
  "unit": "export"
}
```
