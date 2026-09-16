# riverhog_client.ApiClient.list_processing_claim_artifacts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-list-processing-21c4f1378c:a3f1dd7778 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1fbb93fee3"></a>
- <a id="s-e9d6712180"></a>`distribution`: `riverhog-client`
- <a id="s-7026ce507e"></a>`module`: `riverhog_client`
- <a id="s-a33750d2d9"></a>`name`: `list_processing_claim_artifacts`
- <a id="s-8069dc4715"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-2bf72c51bc"></a>`unit`: `member`

### Declared structure

- <a id="s-3da533d8a2"></a>`kind`: `"method"`
- <a id="s-c09cd63ef8"></a>`signature`: `"\"(self, claim_id: 'ProcessingClaimId', *, authority_sha256: 'str', start_ordinal: 'int' = 0) -> 'CollectionArtifactPageDocument'\""`

## Maintained corroboration

### Related interface records

- [GET /v1/collection-processing-claims/{claim_id}/plan/artifacts](../../riverhog/http-operations/get-v1-collection-processing-claims-claim-id-plan-artifacts.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-d4e9fd043f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`
- **Client method:** [packages/riverhog-client/src/riverhog_client/workflows.py::CollectionWorkflowMethods.list_processing_claim_artifacts](../../../../../../packages/riverhog-client/src/riverhog_client/workflows.py#L362)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.list_processing_claim_artifacts`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3e122388eb893959bbe77e501b9da96688426450812cf33853b3b77d60e55dce -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'ProcessingClaimId', *, authority_sha256: 'str', start_ordinal: 'int' = 0) -> 'CollectionArtifactPageDocument'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "list_processing_claim_artifacts",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```
