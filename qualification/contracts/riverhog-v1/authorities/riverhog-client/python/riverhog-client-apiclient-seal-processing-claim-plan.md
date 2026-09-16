# riverhog_client.ApiClient.seal_processing_claim_plan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-seal-processing-claim-plan:4116db8e45 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f60563ced4"></a>
- <a id="s-d0dee2896e"></a>`distribution`: `riverhog-client`
- <a id="s-0ac68cb05e"></a>`module`: `riverhog_client`
- <a id="s-6b25e3db75"></a>`name`: `seal_processing_claim_plan`
- <a id="s-f706e9082c"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-aa67e9966e"></a>`unit`: `member`

### Declared structure

- <a id="s-a909cd6340"></a>`kind`: `"method"`
- <a id="s-f40d085c8b"></a>`signature`: `"\"(self, claim_id: 'ProcessingClaimId', *, fence: 'int', execution_id: 'str', controller_evidence: 'Mapping[str, Any]', controller_evidence_sha256: 'str', operation_id: 'str', operation_sha256: 'str', input_artifacts: 'Iterable[ArtifactInput]', retirement_policy: 'RetirementPolicy' = 'retain', retirement_grace_seconds: 'int' = 0) -> 'ProcessingClaimDocument'\""`

## Maintained corroboration

### Related interface records

- [POST /v1/collection-processing-claims/{claim_id}/plan](../../riverhog/http-operations/post-v1-collection-processing-claims-claim-id-plan.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-fcb895f645"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`
- **Client method:** [packages/riverhog-client/src/riverhog_client/workflows.py::CollectionWorkflowMethods.seal_processing_claim_plan](../../../../../../packages/riverhog-client/src/riverhog_client/workflows.py#L287)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.seal_processing_claim_plan`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1bb75e172f18cdf93f835ba3b5c77201f4c7a610daca22adfcf62b16115d73d4 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'ProcessingClaimId', *, fence: 'int', execution_id: 'str', controller_evidence: 'Mapping[str, Any]', controller_evidence_sha256: 'str', operation_id: 'str', operation_sha256: 'str', input_artifacts: 'Iterable[ArtifactInput]', retirement_policy: 'RetirementPolicy' = 'retain', retirement_grace_seconds: 'int' = 0) -> 'ProcessingClaimDocument'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "seal_processing_claim_plan",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```
