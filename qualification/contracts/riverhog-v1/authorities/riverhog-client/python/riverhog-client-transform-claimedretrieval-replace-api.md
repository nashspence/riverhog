# riverhog_client.transform.ClaimedRetrieval.replace_api

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-claimedretrieva-b80b0c6a05:ff7d43dcd7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-eef3d1507e"></a>
| Field | Shape |
|---|---|
| <a id="s-10aa5655d4"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-4cf43a3003"></a>`distribution` | "riverhog-client" |
| <a id="s-69de4069fc"></a>`module` | "riverhog_client.transform" |
| <a id="s-a5fada7dcc"></a>`name` | "replace_api" |
| <a id="s-03a355eb63"></a>`owner` | "riverhog_client.transform.ClaimedRetrieval" |
| <a id="s-8ca9242bf0"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_client.transform.ClaimedRetrieval](riverhog-client-transform-claimedretrieval.md)

## Governing policies

- <a id="pa-851a15e6f7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.ClaimedRetrieval.replace_api`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 465f6419e921281630cda54f691e6e3a70f3a4f43e6017920a516a3a16b645b9 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, api: 'ClaimedCollectionApi') -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "replace_api",
  "owner": "riverhog_client.transform.ClaimedRetrieval",
  "unit": "member"
}
```
