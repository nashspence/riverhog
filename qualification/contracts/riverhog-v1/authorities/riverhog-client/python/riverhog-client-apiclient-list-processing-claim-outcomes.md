# riverhog_client.ApiClient.list_processing_claim_outcomes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-list-processing-6c62c40830:5fbee85a02 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c90825bd92"></a>
- <a id="s-56f7c39bcf"></a>`distribution`: `riverhog-client`
- <a id="s-fdc6203cfc"></a>`module`: `riverhog_client`
- <a id="s-43e45a5a1b"></a>`name`: `list_processing_claim_outcomes`
- <a id="s-abd2e21a8b"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-f6a2897bea"></a>`unit`: `member`

### Declared structure

- <a id="s-20d966fd49"></a>`kind`: `"method"`
- <a id="s-46e0c6799e"></a>`signature`: `"\"(self, claim_id: 'ProcessingClaimId', *, authority_sha256: 'str', start_ordinal: 'int' = 0) -> 'ProcessingOutcomePageDocument'\""`

## Maintained corroboration

### Related interface records

- [GET /v1/collection-processing-claims/{claim_id}/outcomes](../../riverhog/http-operations/get-v1-collection-processing-claims-claim-id-outcomes.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-0c3a5386b6"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/workflows.py::CollectionWorkflowMethods.list\_processing\_claim\_outcomes](../../../../../../packages/riverhog-client/src/riverhog_client/workflows.py#L653)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.list_processing_claim_outcomes`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5acc9a9d708c2bd5e7e255949f03d8db8f14b8d8d4ecacfc116d2819b06783ea -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'ProcessingClaimId', *, authority_sha256: 'str', start_ordinal: 'int' = 0) -> 'ProcessingOutcomePageDocument'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "list_processing_claim_outcomes",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
