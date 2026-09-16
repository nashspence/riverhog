# stove0_api_client.Stove0ApiClient.rebaseline_admission_policy

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-api-client:stove0-api-client-stove0apiclient-rebasel-ef12f7110c:5155bcddf9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f955ee3928"></a>
- <a id="s-cff293e499"></a>`distribution`: `stove0-api-client`
- <a id="s-e7908b61cc"></a>`module`: `stove0_api_client`
- <a id="s-916c56241e"></a>`name`: `rebaseline_admission_policy`
- <a id="s-aa9e58d98f"></a>`owner`: `stove0_api_client.Stove0ApiClient`
- <a id="s-c9c08bab08"></a>`unit`: `member`

### Declared structure

- <a id="s-5da1e1c574"></a>`kind`: `"method"`
- <a id="s-c109846237"></a>`signature`: `"\"(self, policy_id: 'str') -> 'AdmissionPolicyStatus'\""`

## Maintained corroboration

### Related interface records

- [stove0 admission policy rebaseline](../../stove0-client/cli/stove0-admission-policy-rebaseline.md)
- [POST /v1/admission-policies/{policy_id}:rebaseline](../../stove0/http-operations/post-v1-admission-policies-policy-id-rebaseline.md)
- [Stove0ApiClient](stove0-api-client-stove0apiclient.md)

## Governing policies

- <a id="pa-aeb0e7a7e9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-api-client:stove0_api_client](../../../evidence/sources.md#src-5d52ac5998) — `reference/stove0/packages/api-client/src/stove0_api_client/__init__.py`
- **Client method:** [reference/stove0/packages/api-client/src/stove0_api_client/client.py::Stove0ApiClient.rebaseline_admission_policy](../../../../../../reference/stove0/packages/api-client/src/stove0_api_client/client.py#L164)

### Machine authority

- `/external_contract/python/stove0_api_client.Stove0ApiClient.rebaseline_admission_policy`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9ef1492f96019d91c0eca1773da91f2c4855119b91ac814f455ec78e52085a4b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, policy_id: 'str') -> 'AdmissionPolicyStatus'\""
  },
  "distribution": "stove0-api-client",
  "module": "stove0_api_client",
  "name": "rebaseline_admission_policy",
  "owner": "stove0_api_client.Stove0ApiClient",
  "unit": "member"
}
```

</details>
