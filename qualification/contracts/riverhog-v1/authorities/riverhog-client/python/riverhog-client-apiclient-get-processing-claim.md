# riverhog_client.ApiClient.get_processing_claim

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-get-processing-claim:9ede05dddc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d3312ca67d"></a>
- <a id="s-077120d4c2"></a>`distribution`: `riverhog-client`
- <a id="s-e275342a22"></a>`module`: `riverhog_client`
- <a id="s-6c0c58ef1e"></a>`name`: `get_processing_claim`
- <a id="s-fc980918c8"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-8b9e16bb20"></a>`unit`: `member`

### Declared structure

- <a id="s-f6c5c7bc56"></a>`kind`: `"method"`
- <a id="s-51ae1ffbb2"></a>`signature`: `"\"(self, claim_id: 'ProcessingClaimId') -> 'ProcessingClaimDocument'\""`

## Maintained corroboration

### Related interface records

- [GET /v1/collection-processing-claims/{claim_id}](../../riverhog/http-operations/get-v1-collection-processing-claims-claim-id.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-14f946a484"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/workflows.py::CollectionWorkflowMethods.get\_processing\_claim](../../../../../../packages/riverhog-client/src/riverhog_client/workflows.py#L216)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.get_processing_claim`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a9a139e3aeb8fe3efd56bcbe3fad3de215981fe9cc6536a6bf7218500695bcc5 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'ProcessingClaimId') -> 'ProcessingClaimDocument'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "get_processing_claim",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
