# riverhog_client.ApiClient.list_processing_claim_inputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-list-processing-2cbb88a097:c016e91351 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f495b62d31"></a>
- <a id="s-643ef75f7b"></a>`distribution`: `riverhog-client`
- <a id="s-3dba39fcc4"></a>`module`: `riverhog_client`
- <a id="s-ed38f4f592"></a>`name`: `list_processing_claim_inputs`
- <a id="s-a1caad9a1e"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-de7f44f0b3"></a>`unit`: `member`

### Declared structure

- <a id="s-12044e54ac"></a>`kind`: `"method"`
- <a id="s-06ca445393"></a>`signature`: `"\"(self, claim_id: 'ProcessingClaimId', *, authority_sha256: 'str', start_ordinal: 'int' = 0) -> 'CollectionRootPageDocument'\""`

## Maintained corroboration

### Related interface records

- [GET /v1/collection-processing-claims/{claim_id}/inputs](../../riverhog/http-operations/get-v1-collection-processing-claims-claim-id-inputs.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-7df4a45026"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/workflows.py::CollectionWorkflowMethods.list\_processing\_claim\_inputs](../../../../../../packages/riverhog-client/src/riverhog_client/workflows.py#L197)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.list_processing_claim_inputs`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 504335201d5a8f34cdfbf0ff72a61bc879c2494bbc4b494b18e3aa93f1c457e3 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'ProcessingClaimId', *, authority_sha256: 'str', start_ordinal: 'int' = 0) -> 'CollectionRootPageDocument'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "list_processing_claim_inputs",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
