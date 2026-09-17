# riverhog_client.ApiClient.record_processing_claim_disposition_outputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-record-processi-46baf4c907:032c7b5727 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e94ac9548c"></a>
- <a id="s-73671ef992"></a>`distribution`: `riverhog-client`
- <a id="s-97de833513"></a>`module`: `riverhog_client`
- <a id="s-fb99236b75"></a>`name`: `record_processing_claim_disposition_outputs`
- <a id="s-182a161d4a"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-5dbadf2a03"></a>`unit`: `member`

### Declared structure

- <a id="s-d591cb5058"></a>`kind`: `"method"`
- <a id="s-edce875b45"></a>`signature`: `"\"(self, claim_id: 'ProcessingClaimId', *, fence: 'int', outputs: 'Sequence[DispositionOutputInput]') -> 'ArtifactDispositionSetDocument'\""`

## Maintained corroboration

### Related interface records

- [PUT /v1/collection-processing-claims/{claim_id}/derivation/output-edges](../../riverhog/http-operations/put-v1-collection-processing-claims-claim-id-derivation-output-edges.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-9b1b68f297"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/workflows.py::CollectionWorkflowMethods.record\_processing\_claim\_disposition\_outputs](../../../../../../packages/riverhog-client/src/riverhog_client/workflows.py#L535)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.record_processing_claim_disposition_outputs`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f6c53725bdefcfdbd0d2eb0ae10d132aa29a0c667295b1fadddcf563bd1426c4 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'ProcessingClaimId', *, fence: 'int', outputs: 'Sequence[DispositionOutputInput]') -> 'ArtifactDispositionSetDocument'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "record_processing_claim_disposition_outputs",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
