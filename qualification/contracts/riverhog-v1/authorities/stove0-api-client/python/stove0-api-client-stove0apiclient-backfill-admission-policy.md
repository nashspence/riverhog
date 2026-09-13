# stove0_api_client.Stove0ApiClient.backfill_admission_policy

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-api-client:stove0-api-client-stove0apiclient-backfil-199a1c6121:144e06cb88 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e436cb4202"></a>
| Field | Shape |
|---|---|
| <a id="s-6f292b9fd0"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-a0902debca"></a>`distribution` | "stove0-api-client" |
| <a id="s-86bc9526a3"></a>`module` | "stove0_api_client" |
| <a id="s-10f3e46218"></a>`name` | "backfill_admission_policy" |
| <a id="s-5021475eb7"></a>`owner` | "stove0_api_client.Stove0ApiClient" |
| <a id="s-7980fc86a9"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_api_client.Stove0ApiClient](stove0-api-client-stove0apiclient.md)

## Governing policies

- <a id="pa-e785b09491"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-api-client:stove0_api_client](../../../evidence/sources.md#src-5d52ac5998) — `reference/stove0/packages/api-client/src/stove0_api_client/__init__.py`

### Machine authority

- `/external_contract/python/stove0_api_client.Stove0ApiClient.backfill_admission_policy`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5bc6050180552d44b55361f5cae176a9b2c7e4707481ea25a1314eaaee27e253 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, policy_id: 'str') -> 'AdmissionPolicyStatus'\""
  },
  "distribution": "stove0-api-client",
  "module": "stove0_api_client",
  "name": "backfill_admission_policy",
  "owner": "stove0_api_client.Stove0ApiClient",
  "unit": "member"
}
```
