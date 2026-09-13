# riverhog_client.transform.CapabilityApiClient.replace

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-capabilityapicl-2f0f77cead:962df93267 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-724da998e0"></a>
| Field | Shape |
|---|---|
| <a id="s-2bcad04b99"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-a4857a651a"></a>`distribution` | "riverhog-client" |
| <a id="s-a53d96d0e7"></a>`module` | "riverhog_client.transform" |
| <a id="s-cf93b36425"></a>`name` | "replace" |
| <a id="s-5430519073"></a>`owner` | "riverhog_client.transform.CapabilityApiClient" |
| <a id="s-b8777df192"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_client.transform.CapabilityApiClient](riverhog-client-transform-capabilityapiclient.md)

## Governing policies

- <a id="pa-1d1ab5de46"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.CapabilityApiClient.replace`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e0153f4d9378e7185988be7669b95cab923d8d7bfbdce3959ff061e2b891ed23 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, client: 'Any', *, owns_client: 'bool' = True) -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "replace",
  "owner": "riverhog_client.transform.CapabilityApiClient",
  "unit": "member"
}
```
