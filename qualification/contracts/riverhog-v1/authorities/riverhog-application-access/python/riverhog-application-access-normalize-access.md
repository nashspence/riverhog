# riverhog_application_access.normalize_access

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-normalize-access:5f89a041f1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-837e5c9b34"></a>
| Field | Shape |
|---|---|
| <a id="s-39faf3938c"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-8c188aea5e"></a>`distribution` | "riverhog-application-access" |
| <a id="s-342b048139"></a>`module` | "riverhog_application_access" |
| <a id="s-90e859d83b"></a>`name` | "normalize_access" |
| <a id="s-a0ee228a55"></a>`unit` | "export" |

## Governing policies

- <a id="pa-7cbac118ef"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources.md#src-9d9ce5fdac) — `packages/riverhog-application-access/src/riverhog_application_access/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_application_access.normalize_access`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 562f8561fa41464dbe7c527e7828270cc651c97ce52b7d13b1ac2eb77ef1a6b5 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(values: 'Iterable[ApplicationAccess | tuple[str, str]]') -> 'tuple[ApplicationAccess, ...]'\""
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "normalize_access",
  "unit": "export"
}
```
