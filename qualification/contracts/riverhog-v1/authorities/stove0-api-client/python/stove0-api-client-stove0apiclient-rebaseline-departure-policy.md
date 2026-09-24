# stove0_api_client.Stove0ApiClient.rebaseline_departure_policy

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-api-client:stove0-api-client-stove0apiclient-rebasel-e340562ecc:1c420dbbdc -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a4c41f3ba5"></a>
- <a id="s-ad176d927f"></a>`distribution`: `stove0-api-client`
- <a id="s-731d93725f"></a>`module`: `stove0_api_client`
- <a id="s-8c6f0c43a8"></a>`name`: `rebaseline_departure_policy`
- <a id="s-e2b9b79662"></a>`owner`: `stove0_api_client.Stove0ApiClient`
- <a id="s-9168b30622"></a>`unit`: `member`

### Declared structure

- <a id="s-3ac7d2b378"></a>`kind`: `"method"`
- <a id="s-198e8dd66b"></a>`signature`: `"\"(self, policy_id: 'str') -> 'DeparturePolicyStatus'\""`

## Maintained corroboration

### Related interface records

- [stove0 departure policy rebaseline](../../a-stove0-cli/cli/stove0-departure-policy-rebaseline.md)
- [POST /v1/departure-policies/{policy_id}:rebaseline](../../stove0/http-operations/post-v1-departure-policies-policy-id-rebaseline.md)
- [Stove0ApiClient](stove0-api-client-stove0apiclient.md)

## Governing policies

- <a id="pa-15b3fade24"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-api-client:stove0_api_client](../../../evidence/sources/authorities.md#src-5d52ac5998) — [some-implementations/stove0/packages/api-client/src/stove0\_api\_client/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/api-client/src/stove0_api_client/__init__.py)
- **Client method:** [some-implementations/stove0/packages/api-client/src/stove0\_api\_client/client.py::Stove0ApiClient.rebaseline\_departure\_policy](../../../../../../some-implementations/stove0/packages/api-client/src/stove0_api_client/client.py#L228)

### Machine authority

- `/external_contract/python/stove0_api_client.Stove0ApiClient.rebaseline_departure_policy`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d886012bba0dc256ccee487bca66b10264b8a018335fc5cd25367df57e4c25ab -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, policy_id: 'str') -> 'DeparturePolicyStatus'\""
  },
  "distribution": "stove0-api-client",
  "module": "stove0_api_client",
  "name": "rebaseline_departure_policy",
  "owner": "stove0_api_client.Stove0ApiClient",
  "unit": "member"
}
```

</details>
