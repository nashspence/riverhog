# riverhog_client.ApiClient.get_processing_claim_dispositions

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-get-processing-8df735d895:9f098d3004 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d8a93b4a4c"></a>
- <a id="s-893c34cb4d"></a>`distribution`: `riverhog-client`
- <a id="s-0ebd297caf"></a>`module`: `riverhog_client`
- <a id="s-fd2dd2120d"></a>`name`: `get_processing_claim_dispositions`
- <a id="s-b1e00604f3"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-30d18de9df"></a>`unit`: `member`

### Declared structure

- <a id="s-a8139d81ab"></a>`kind`: `"method"`
- <a id="s-1bd040ced1"></a>`signature`: `"\"(self, claim_id: 'ProcessingClaimId') -> 'ArtifactDispositionSetDocument'\""`

## Maintained corroboration

### Related interface records

- [GET /v1/collection-processing-claims/{claim_id}/derivation](../../riverhog/http-operations/get-v1-collection-processing-claims-claim-id-derivation.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-bbf303fcfc"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`
- **Client method:** [packages/riverhog-client/src/riverhog_client/workflows.py::CollectionWorkflowMethods.get_processing_claim_dispositions](../../../../../../packages/riverhog-client/src/riverhog_client/workflows.py#L595)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.get_processing_claim_dispositions`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6217b348130d781ae64439f1ea4a9bd475945a3d50779c3a137cc5a2249a1d81 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'ProcessingClaimId') -> 'ArtifactDispositionSetDocument'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "get_processing_claim_dispositions",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```
