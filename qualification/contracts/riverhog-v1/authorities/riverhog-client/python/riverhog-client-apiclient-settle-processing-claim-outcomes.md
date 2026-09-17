# riverhog_client.ApiClient.settle_processing_claim_outcomes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-settle-processi-fe4ac4d76a:53cfe70065 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c0ad33acab"></a>
- <a id="s-1c04d96343"></a>`distribution`: `riverhog-client`
- <a id="s-424c9af057"></a>`module`: `riverhog_client`
- <a id="s-f31477c55b"></a>`name`: `settle_processing_claim_outcomes`
- <a id="s-148ff63f2b"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-7b2d08e2fe"></a>`unit`: `member`

### Declared structure

- <a id="s-2aa28ca419"></a>`kind`: `"method"`
- <a id="s-f61515180d"></a>`signature`: `"\"(self, claim_id: 'ProcessingClaimId', *, fence: 'int', retirement_policy: 'RetirementPolicy' = 'retain', retirement_grace_seconds: 'int' = 0) -> 'ProcessingClaimDocument'\""`

## Maintained corroboration

### Related interface records

- [POST /v1/collection-processing-claims/{claim_id}/outcomes/settle](../../riverhog/http-operations/post-v1-collection-processing-claims-claim-id-outcomes-settle.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-065b0a4246"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/workflows.py::CollectionWorkflowMethods.settle\_processing\_claim\_outcomes](../../../../../../packages/riverhog-client/src/riverhog_client/workflows.py#L607)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.settle_processing_claim_outcomes`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fe5e547869dd162d58e67f01ffaa823f5e3a5107cecdcf2d07900463c24526dc -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'ProcessingClaimId', *, fence: 'int', retirement_policy: 'RetirementPolicy' = 'retain', retirement_grace_seconds: 'int' = 0) -> 'ProcessingClaimDocument'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "settle_processing_claim_outcomes",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
