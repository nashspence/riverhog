# riverhog_client.transform.ClaimedRetrieval.closed

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-claimedretrieval-closed:2939826d81 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bce442841d"></a>
| Field | Shape |
|---|---|
| <a id="s-367257f610"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-ae67883832"></a>`distribution` | "riverhog-client" |
| <a id="s-f245a12eee"></a>`module` | "riverhog_client.transform" |
| <a id="s-73436c5a39"></a>`name` | "closed" |
| <a id="s-670a29e184"></a>`owner` | "riverhog_client.transform.ClaimedRetrieval" |
| <a id="s-af5c0a64dc"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_client.transform.ClaimedRetrieval](riverhog-client-transform-claimedretrieval.md)

## Governing policies

- <a id="pa-d271a8258e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.ClaimedRetrieval.closed`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 47415d5c8a847564a7943a4a336ac5b8800370694b797d1de34d4bb1dea23b4e -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> 'bool'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "closed",
  "owner": "riverhog_client.transform.ClaimedRetrieval",
  "unit": "member"
}
```
