# riverhog_client.ApiClient.seal_processing_claim_dispositions

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-seal-processing-2e2ff67c84:09a89e0663 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3fff2d1697"></a>
- <a id="s-94512a233b"></a>`distribution`: `riverhog-client`
- <a id="s-40fd4bb554"></a>`module`: `riverhog_client`
- <a id="s-4b96a3a6d5"></a>`name`: `seal_processing_claim_dispositions`
- <a id="s-1d2564d3d2"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-6ec6a712e1"></a>`unit`: `member`

### Declared structure

- <a id="s-e021b4d3d9"></a>`kind`: `"method"`
- <a id="s-1b64b2686b"></a>`signature`: `"\"(self, claim_id: 'ProcessingClaimId', *, fence: 'int') -> 'ArtifactDispositionSetDocument'\""`

## Maintained corroboration

### Related interface records

- [POST /v1/collection-processing-claims/{claim_id}/derivation/seal](../../riverhog/http-operations/post-v1-collection-processing-claims-claim-id-derivation-seal.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-db05c42667"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`
- **Client method:** [packages/riverhog-client/src/riverhog_client/workflows.py::CollectionWorkflowMethods.seal_processing_claim_dispositions](../../../../../../packages/riverhog-client/src/riverhog_client/workflows.py#L579)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.seal_processing_claim_dispositions`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 272d9712d18d02f70f5e56b7d149170b6727df671b92b6f8682b2c879886aabb -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'ProcessingClaimId', *, fence: 'int') -> 'ArtifactDispositionSetDocument'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "seal_processing_claim_dispositions",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
