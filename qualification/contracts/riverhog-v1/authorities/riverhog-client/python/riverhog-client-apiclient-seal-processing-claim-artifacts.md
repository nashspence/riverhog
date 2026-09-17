# riverhog_client.ApiClient.seal_processing_claim_artifacts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-seal-processing-bc3e881c73:9ad3d8af6f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-66c0ce2d33"></a>
- <a id="s-88889c9aa3"></a>`distribution`: `riverhog-client`
- <a id="s-1dd0995da7"></a>`module`: `riverhog_client`
- <a id="s-72f26cb57e"></a>`name`: `seal_processing_claim_artifacts`
- <a id="s-0c759b04a7"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-80df9b28b7"></a>`unit`: `member`

### Declared structure

- <a id="s-550935912e"></a>`kind`: `"method"`
- <a id="s-ce9c2da07f"></a>`signature`: `"\"(self, claim_id: 'ProcessingClaimId', *, fence: 'int') -> 'ArtifactReceivingSetDocument'\""`

## Maintained corroboration

### Related interface records

- [POST /v1/collection-processing-claims/{claim_id}/plan/artifacts/seal](../../riverhog/http-operations/post-v1-collection-processing-claims-claim-id-plan-artifacts-seal.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-b7d84f5789"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/workflows.py::CollectionWorkflowMethods.seal\_processing\_claim\_artifacts](../../../../../../packages/riverhog-client/src/riverhog_client/workflows.py#L347)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.seal_processing_claim_artifacts`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 13d45d92afbc99a71af6fa17285ebd2c401b7a795ffaf866350067a9ea60106d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'ProcessingClaimId', *, fence: 'int') -> 'ArtifactReceivingSetDocument'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "seal_processing_claim_artifacts",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
