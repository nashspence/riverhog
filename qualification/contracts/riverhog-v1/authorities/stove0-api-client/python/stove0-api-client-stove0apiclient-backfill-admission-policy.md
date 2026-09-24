# stove0_api_client.Stove0ApiClient.backfill_admission_policy

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-api-client:stove0-api-client-stove0apiclient-backfil-199a1c6121:144e06cb88 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e436cb4202"></a>
- <a id="s-a0902debca"></a>`distribution`: `stove0-api-client`
- <a id="s-86bc9526a3"></a>`module`: `stove0_api_client`
- <a id="s-10f3e46218"></a>`name`: `backfill_admission_policy`
- <a id="s-5021475eb7"></a>`owner`: `stove0_api_client.Stove0ApiClient`
- <a id="s-7980fc86a9"></a>`unit`: `member`

### Declared structure

- <a id="s-0c7dc51fc1"></a>`kind`: `"method"`
- <a id="s-9a13aa6ed4"></a>`signature`: `"\"(self, policy_id: 'str') -> 'AdmissionPolicyStatus'\""`

## Maintained corroboration

### Related interface records

- [stove0 admission policy backfill](../../a-stove0-cli/cli/stove0-admission-policy-backfill.md)
- [POST /v1/admission-policies/{policy_id}:backfill](../../stove0/http-operations/post-v1-admission-policies-policy-id-backfill.md)
- [Stove0ApiClient](stove0-api-client-stove0apiclient.md)

## Governing policies

- <a id="pa-e785b09491"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-api-client:stove0_api_client](../../../evidence/sources/authorities.md#src-5d52ac5998) — [some-implementations/stove0/packages/api-client/src/stove0\_api\_client/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/api-client/src/stove0_api_client/__init__.py)
- **Client method:** [some-implementations/stove0/packages/api-client/src/stove0\_api\_client/client.py::Stove0ApiClient.backfill\_admission\_policy](../../../../../../some-implementations/stove0/packages/api-client/src/stove0_api_client/client.py#L177)

### Machine authority

- `/external_contract/python/stove0_api_client.Stove0ApiClient.backfill_admission_policy`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
