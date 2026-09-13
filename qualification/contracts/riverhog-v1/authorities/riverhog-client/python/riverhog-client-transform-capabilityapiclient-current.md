# riverhog_client.transform.CapabilityApiClient.current

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-capabilityapicl-0fd5c971c8:4f790d8d0a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-477bafc105"></a>
| Field | Shape |
|---|---|
| <a id="s-9224e8270d"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-6e6d16baf3"></a>`distribution` | "riverhog-client" |
| <a id="s-bcba2320b5"></a>`module` | "riverhog_client.transform" |
| <a id="s-7c5f2c9187"></a>`name` | "current" |
| <a id="s-820a619009"></a>`owner` | "riverhog_client.transform.CapabilityApiClient" |
| <a id="s-20cefb2c3f"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_client.transform.CapabilityApiClient](riverhog-client-transform-capabilityapiclient.md)

## Governing policies

- <a id="pa-75189bbd75"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.CapabilityApiClient.current`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dca7f41245f61e18786c9f233b4f84ec5453e49bcedc843a7910e2347545632c -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> 'Any'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "current",
  "owner": "riverhog_client.transform.CapabilityApiClient",
  "unit": "member"
}
```
