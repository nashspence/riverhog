# riverhog_client.transform.CapabilityApiClient.close

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-capabilityapiclient-close:b8e69514bd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-54ac13be5c"></a>
| Field | Shape |
|---|---|
| <a id="s-3a6cb6b840"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-532c5ece75"></a>`distribution` | "riverhog-client" |
| <a id="s-bc920859dd"></a>`module` | "riverhog_client.transform" |
| <a id="s-f44e941328"></a>`name` | "close" |
| <a id="s-0b558340c1"></a>`owner` | "riverhog_client.transform.CapabilityApiClient" |
| <a id="s-1962a49759"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_client.transform.CapabilityApiClient](riverhog-client-transform-capabilityapiclient.md)

## Governing policies

- <a id="pa-1012983f86"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.CapabilityApiClient.close`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: aa4c93136daa752df741c385ed9048e04f9cf20778571e2167c121803592e0a0 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "close",
  "owner": "riverhog_client.transform.CapabilityApiClient",
  "unit": "member"
}
```
