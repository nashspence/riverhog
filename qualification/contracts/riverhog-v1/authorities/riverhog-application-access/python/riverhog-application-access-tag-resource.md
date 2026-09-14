# riverhog_application_access.tag_resource

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-tag-resource:ada5965b7d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-316b7740c2"></a>
- <a id="s-e414ec64e2"></a>`distribution`: `riverhog-application-access`
- <a id="s-9363fe5180"></a>`module`: `riverhog_application_access`
- <a id="s-575dd1e040"></a>`name`: `tag_resource`
- <a id="s-e9a071053c"></a>`unit`: `export`

### Declared structure

- <a id="s-62eb7d15dc"></a>`kind`: `"function"`
- <a id="s-72884f8921"></a>`signature`: `"\"(tag: 'str') -> 'str'\""`

## Governing policies

- <a id="pa-4356b9a2bc"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources.md#src-9d9ce5fdac) — `packages/riverhog-application-access/src/riverhog_application_access/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_application_access.tag_resource`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cd8001a883bb332d75b2904e3f9584d1a8e89646988dece71362f8042a954d18 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(tag: 'str') -> 'str'\""
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "tag_resource",
  "unit": "export"
}
```
