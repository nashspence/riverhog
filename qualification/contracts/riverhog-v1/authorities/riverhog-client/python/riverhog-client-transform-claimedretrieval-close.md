# riverhog_client.transform.ClaimedRetrieval.close

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-claimedretrieval-close:fcada33436 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-34f11d3148"></a>
| Field | Shape |
|---|---|
| <a id="s-021795e7ff"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-d686193dc4"></a>`distribution` | "riverhog-client" |
| <a id="s-9c8176ef65"></a>`module` | "riverhog_client.transform" |
| <a id="s-4c62fb2310"></a>`name` | "close" |
| <a id="s-7d3aa75d6d"></a>`owner` | "riverhog_client.transform.ClaimedRetrieval" |
| <a id="s-dd97639916"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_client.transform.ClaimedRetrieval](riverhog-client-transform-claimedretrieval.md)

## Governing policies

- <a id="pa-936f29b898"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.ClaimedRetrieval.close`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bacea339f3d8de4c06947eabc5c6673341f38c84d298316c867b4fea12e51aeb -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, success: 'bool' = True) -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "close",
  "owner": "riverhog_client.transform.ClaimedRetrieval",
  "unit": "member"
}
```
