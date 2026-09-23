# riverhog_client.ApiClient.settle_processing_claim

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-settle-processing-claim:a24ab540a1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-96df7adcb6"></a>
- <a id="s-93418f11d1"></a>`distribution`: `riverhog-client`
- <a id="s-8b34979055"></a>`module`: `riverhog_client`
- <a id="s-73bbb09546"></a>`name`: `settle_processing_claim`
- <a id="s-186a9c66b0"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-d30ab160f5"></a>`unit`: `member`

### Declared structure

- <a id="s-2072b53422"></a>`kind`: `"method"`
- <a id="s-12820faf4a"></a>`signature`: `"\"(self, claim_id: 'ProcessingClaimId', *, fence: 'int', output_collection_id: 'CollectionId', derivation: 'DerivationInput', outcome_claim_id: 'ProcessingClaimId \| None' = None, outcome_fence: 'int \| None' = None, outcome_id: 'str \| None' = None) -> 'ProcessingClaimDocument'\""`

## Maintained corroboration

### Related interface records

- [POST /v1/collection-processing-claims/{claim_id}/settle](../../riverhog/http-operations/post-v1-collection-processing-claims-claim-id-settle.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-1e483af328"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/workflows.py::CollectionWorkflowMethods.settle\_processing\_claim](../../../../../../packages/riverhog-client/src/riverhog_client/workflows.py#L485)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.settle_processing_claim`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2809b077502dea31fa25ebd3e5666a5b3d7b5ae6ad29ce482519670e35180bdf -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'ProcessingClaimId', *, fence: 'int', output_collection_id: 'CollectionId', derivation: 'DerivationInput', outcome_claim_id: 'ProcessingClaimId | None' = None, outcome_fence: 'int | None' = None, outcome_id: 'str | None' = None) -> 'ProcessingClaimDocument'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "settle_processing_claim",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
