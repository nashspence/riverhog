# riverhog_client.ApiClient.release_processing_claim

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-release-processing-claim:acd1fd0c9f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b2e93b8501"></a>
- <a id="s-865bd4f14a"></a>`distribution`: `riverhog-client`
- <a id="s-010b9d4358"></a>`module`: `riverhog_client`
- <a id="s-7632d111e5"></a>`name`: `release_processing_claim`
- <a id="s-f9c77a1866"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-9cfab085cb"></a>`unit`: `member`

### Declared structure

- <a id="s-a7432e34e8"></a>`kind`: `"method"`
- <a id="s-7b00bc0f27"></a>`signature`: `"\"(self, claim_id: 'ProcessingClaimId', *, fence: 'int') -> 'ProcessingClaimDocument'\""`

## Maintained corroboration

### Related interface records

- [POST /v1/collection-processing-claims/{claim_id}/release](../../riverhog/http-operations/post-v1-collection-processing-claims-claim-id-release.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-d739735bfa"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/workflows.py::CollectionWorkflowMethods.release\_processing\_claim](../../../../../../packages/riverhog-client/src/riverhog_client/workflows.py#L659)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.release_processing_claim`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b5bbfed4eb11a1f35312fc9aeba3679a76be46cc578bf8112fbfb162e04ed9fd -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'ProcessingClaimId', *, fence: 'int') -> 'ProcessingClaimDocument'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "release_processing_claim",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
