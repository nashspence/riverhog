# riverhog_client.ApiClient.seal_transform_capability_artifacts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-seal-transform-1ea2bd3522:f0c6713a84 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-507da2604f"></a>
- <a id="s-6d15777a35"></a>`distribution`: `riverhog-client`
- <a id="s-a96bbaa8a8"></a>`module`: `riverhog_client`
- <a id="s-78fa31db4f"></a>`name`: `seal_transform_capability_artifacts`
- <a id="s-b648dedb5b"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-66721abbcf"></a>`unit`: `member`

### Declared structure

- <a id="s-73d7fe448d"></a>`kind`: `"method"`
- <a id="s-5c12cc8f6f"></a>`signature`: `"\"(self, claim_id: 'ProcessingClaimId', capability_id: 'str', *, fence: 'int') -> 'ArtifactReceivingSetDocument'\""`

## Maintained corroboration

### Related interface records

- [POST /v1/collection-processing-claims/{claim_id}/capabilities/{capability_id}/artifacts/seal](../../riverhog/http-operations/post-v1-collection-processing-claims-claim-id-capabilities-capability-id-artifacts-seal.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-22472a0fae"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/workflows.py::CollectionWorkflowMethods.seal\_transform\_capability\_artifacts](../../../../../../packages/riverhog-client/src/riverhog_client/workflows.py#L448)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.seal_transform_capability_artifacts`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 805b684d6ead3768925b9c746764b4d8fd090ee98e0d2ef0751e8e7aebbcc151 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'ProcessingClaimId', capability_id: 'str', *, fence: 'int') -> 'ArtifactReceivingSetDocument'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "seal_transform_capability_artifacts",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
