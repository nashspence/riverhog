# riverhog_client.ApiClient.list_processing_claim_disposition_outputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-list-processing-a0d594f006:de25ecdf46 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b40c78dfbb"></a>
- <a id="s-aa95a515af"></a>`distribution`: `riverhog-client`
- <a id="s-723f2d38e8"></a>`module`: `riverhog_client`
- <a id="s-1938585ee5"></a>`name`: `list_processing_claim_disposition_outputs`
- <a id="s-c63b37018f"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-d680f549cd"></a>`unit`: `member`

### Declared structure

- <a id="s-6032240116"></a>`kind`: `"method"`
- <a id="s-65a3262688"></a>`signature`: `"\"(self, claim_id: 'ProcessingClaimId', *, authority_sha256: 'str', start_ordinal: 'int' = 0) -> 'ArtifactDispositionOutputPageDocument'\""`

## Maintained corroboration

### Related interface records

- [GET /v1/collection-processing-claims/{claim_id}/derivation/output-edges](../../riverhog/http-operations/get-v1-collection-processing-claims-claim-id-derivation-output-edges.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-9515188a56"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/workflows.py::CollectionWorkflowMethods.list\_processing\_claim\_disposition\_outputs](../../../../../../packages/riverhog-client/src/riverhog_client/workflows.py#L585)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.list_processing_claim_disposition_outputs`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1962b5fb6a5cbb5de1edfa9711ee9aeabe50af304a9ef6f349d3a710c666654c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'ProcessingClaimId', *, authority_sha256: 'str', start_ordinal: 'int' = 0) -> 'ArtifactDispositionOutputPageDocument'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "list_processing_claim_disposition_outputs",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
