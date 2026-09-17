# riverhog_client.ApiClient.seal_processing_claim_inputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-seal-processing-a693332a1f:da8075d884 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ce6e6ee2d7"></a>
- <a id="s-69d30791a9"></a>`distribution`: `riverhog-client`
- <a id="s-1655e9fa97"></a>`module`: `riverhog_client`
- <a id="s-f3f5474d00"></a>`name`: `seal_processing_claim_inputs`
- <a id="s-b50f22567e"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-f5cc676b83"></a>`unit`: `member`

### Declared structure

- <a id="s-a7edf48b62"></a>`kind`: `"method"`
- <a id="s-d4d76908e7"></a>`signature`: `"\"(self, claim_id: 'ProcessingClaimId', *, fence: 'int') -> 'ReceivingSetDocument'\""`

## Maintained corroboration

### Related interface records

- [POST /v1/collection-processing-claims/{claim_id}/inputs/seal](../../riverhog/http-operations/post-v1-collection-processing-claims-claim-id-inputs-seal.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-90ff80f380"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/workflows.py::CollectionWorkflowMethods.seal\_processing\_claim\_inputs](../../../../../../packages/riverhog-client/src/riverhog_client/workflows.py#L182)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.seal_processing_claim_inputs`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e88d42aafacb42f64db32ed09160339dd2d744fe2fe916c7d0cdc73eb37a0c94 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'ProcessingClaimId', *, fence: 'int') -> 'ReceivingSetDocument'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "seal_processing_claim_inputs",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
