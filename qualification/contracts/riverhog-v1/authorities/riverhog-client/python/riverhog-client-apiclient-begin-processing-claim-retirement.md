# riverhog_client.ApiClient.begin_processing_claim_retirement

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-begin-processin-29fb822cb9:3e9fdd8c8a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-861d5f5d6f"></a>
- <a id="s-597469002c"></a>`distribution`: `riverhog-client`
- <a id="s-0bd5e62abb"></a>`module`: `riverhog_client`
- <a id="s-87708a1f8e"></a>`name`: `begin_processing_claim_retirement`
- <a id="s-653c163a3a"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-5bfa8f6fa4"></a>`unit`: `member`

### Declared structure

- <a id="s-a2fb56e486"></a>`kind`: `"method"`
- <a id="s-6f740a2a3c"></a>`signature`: `"\"(self, claim_id: 'ProcessingClaimId', *, fence: 'int') -> 'ProcessingClaimDocument'\""`

## Maintained corroboration

### Related interface records

- [POST /v1/collection-processing-claims/{claim_id}/retirement](../../riverhog/http-operations/post-v1-collection-processing-claims-claim-id-retirement.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-503b6071c4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`
- **Client method:** [packages/riverhog-client/src/riverhog_client/workflows.py::CollectionWorkflowMethods.begin_processing_claim_retirement](../../../../../../packages/riverhog-client/src/riverhog_client/workflows.py#L646)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.begin_processing_claim_retirement`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: daf86e67ce5364a3b96ed2f29d9563a40def9f8143b01a497d9315b94bbf17c5 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'ProcessingClaimId', *, fence: 'int') -> 'ProcessingClaimDocument'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "begin_processing_claim_retirement",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```
