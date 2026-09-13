# riverhog_client.transform.CapabilityApiClient.spawn

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-capabilityapiclient-spawn:0ec6e67405 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a86f0a473e"></a>
| Field | Shape |
|---|---|
| <a id="s-cdb2677053"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-85098f3eea"></a>`distribution` | "riverhog-client" |
| <a id="s-744aa98368"></a>`module` | "riverhog_client.transform" |
| <a id="s-e640e97e05"></a>`name` | "spawn" |
| <a id="s-7100f7ecee"></a>`owner` | "riverhog_client.transform.CapabilityApiClient" |
| <a id="s-b087e982bf"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_client.transform.CapabilityApiClient](riverhog-client-transform-capabilityapiclient.md)

## Governing policies

- <a id="pa-7ccde02793"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.CapabilityApiClient.spawn`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7c35aac069bd45b9a84de5d08d2ef0aa2e89fcbbfa8334b2d4cc15e3b0630013 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'CapabilityApiClient'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "spawn",
  "owner": "riverhog_client.transform.CapabilityApiClient",
  "unit": "member"
}
```
