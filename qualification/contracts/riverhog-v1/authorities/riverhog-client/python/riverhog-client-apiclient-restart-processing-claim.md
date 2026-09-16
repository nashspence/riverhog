# riverhog_client.ApiClient.restart_processing_claim

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-restart-processing-claim:4214484336 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1c1ca838e6"></a>
- <a id="s-d8b94d57ff"></a>`distribution`: `riverhog-client`
- <a id="s-95b7ed7929"></a>`module`: `riverhog_client`
- <a id="s-3befd3ba2e"></a>`name`: `restart_processing_claim`
- <a id="s-25f9002844"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-fd00da3ac6"></a>`unit`: `member`

### Declared structure

- <a id="s-38c65bef07"></a>`kind`: `"method"`
- <a id="s-e9fd9919aa"></a>`signature`: `"\"(self, claim_id: 'ProcessingClaimId', *, fence: 'int', lease_seconds: 'int' = 1800) -> 'ProcessingClaimDocument'\""`

## Maintained corroboration

### Related interface records

- [POST /v1/collection-processing-claims/{claim_id}/restart](../../riverhog/http-operations/post-v1-collection-processing-claims-claim-id-restart.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-940a549408"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`
- **Client method:** [packages/riverhog-client/src/riverhog_client/workflows.py::CollectionWorkflowMethods.restart_processing_claim](../../../../../../packages/riverhog-client/src/riverhog_client/workflows.py#L263)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.restart_processing_claim`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8294c70e10cce48cc9801b330bc2dfc56af6ad75c5a4de154de1a7f79c2fb3c7 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'ProcessingClaimId', *, fence: 'int', lease_seconds: 'int' = 1800) -> 'ProcessingClaimDocument'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "restart_processing_claim",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```
