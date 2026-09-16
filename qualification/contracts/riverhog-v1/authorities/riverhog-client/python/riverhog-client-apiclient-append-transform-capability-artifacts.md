# riverhog_client.ApiClient.append_transform_capability_artifacts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-append-transfor-c214ae0a47:083494561d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-900f1863ae"></a>
- <a id="s-53289d2a23"></a>`distribution`: `riverhog-client`
- <a id="s-aff30e1dc1"></a>`module`: `riverhog_client`
- <a id="s-8dadefa6e3"></a>`name`: `append_transform_capability_artifacts`
- <a id="s-66c4e7128a"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-694f028c04"></a>`unit`: `member`

### Declared structure

- <a id="s-ae8db08ea0"></a>`kind`: `"method"`
- <a id="s-6b22bb9c0c"></a>`signature`: `"\"(self, claim_id: 'ProcessingClaimId', capability_id: 'str', *, fence: 'int', start_ordinal: 'int', artifacts: 'Sequence[ArtifactInput]') -> 'ArtifactReceivingSetDocument'\""`

## Maintained corroboration

### Related interface records

- [PUT /v1/collection-processing-claims/{claim_id}/capabilities/{capability_id}/artifacts](../../riverhog/http-operations/put-v1-collection-processing-claims-claim-id-capabilities-capability-id-artifacts.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-7538909e62"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`
- **Client method:** [packages/riverhog-client/src/riverhog_client/workflows.py::CollectionWorkflowMethods.append_transform_capability_artifacts](../../../../../../packages/riverhog-client/src/riverhog_client/workflows.py#L422)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.append_transform_capability_artifacts`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f2d1fb52092eb418974d29e08eb77b5db0e99740c16343ee8ee3ba8779fed7b6 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'ProcessingClaimId', capability_id: 'str', *, fence: 'int', start_ordinal: 'int', artifacts: 'Sequence[ArtifactInput]') -> 'ArtifactReceivingSetDocument'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "append_transform_capability_artifacts",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
