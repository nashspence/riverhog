# riverhog_client.transform.ClaimedRetrieval.cleanup_pending

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-claimedretrieva-cce8736baa:e70c0d5f44 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6d7563ac88"></a>
| Field | Shape |
|---|---|
| <a id="s-bbe96d02f9"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-2996c77c88"></a>`distribution` | "riverhog-client" |
| <a id="s-adc1941b82"></a>`module` | "riverhog_client.transform" |
| <a id="s-e7f0587d7b"></a>`name` | "cleanup_pending" |
| <a id="s-e155af48c3"></a>`owner` | "riverhog_client.transform.ClaimedRetrieval" |
| <a id="s-361fe48569"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_client.transform.ClaimedRetrieval](riverhog-client-transform-claimedretrieval.md)

## Governing policies

- <a id="pa-60c26cb3e5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.ClaimedRetrieval.cleanup_pending`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ebfcdcd1957c3b0818039deae67d1cbec9517ddbcb3a93d1c5eb40de995c6204 -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> 'bool'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "cleanup_pending",
  "owner": "riverhog_client.transform.ClaimedRetrieval",
  "unit": "member"
}
```
