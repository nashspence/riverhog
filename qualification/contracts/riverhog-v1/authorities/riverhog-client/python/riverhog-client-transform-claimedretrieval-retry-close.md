# riverhog_client.transform.ClaimedRetrieval.retry_close

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-claimedretrieva-ed9f2535cf:b9a357ae7d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d151600264"></a>
| Field | Shape |
|---|---|
| <a id="s-97b2121875"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-98661f9e58"></a>`distribution` | "riverhog-client" |
| <a id="s-cddbf13d0a"></a>`module` | "riverhog_client.transform" |
| <a id="s-f367783bf5"></a>`name` | "retry_close" |
| <a id="s-7b55fa2d52"></a>`owner` | "riverhog_client.transform.ClaimedRetrieval" |
| <a id="s-19b9b4b37e"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_client.transform.ClaimedRetrieval](riverhog-client-transform-claimedretrieval.md)

## Governing policies

- <a id="pa-1105c5a33e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.ClaimedRetrieval.retry_close`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ed3186bd31fdeffa38f12de0ead062e12a421c9f31b239f94491896ed68dac5b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "retry_close",
  "owner": "riverhog_client.transform.ClaimedRetrieval",
  "unit": "member"
}
```
