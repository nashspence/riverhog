# riverhog_client.ApiClient.record_processing_claim_dispositions

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-record-processi-6ecc55f677:af0d9d3e8c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d552a0579c"></a>
- <a id="s-1b503aa6dd"></a>`distribution`: `riverhog-client`
- <a id="s-ee4f6ab56b"></a>`module`: `riverhog_client`
- <a id="s-639cc23469"></a>`name`: `record_processing_claim_dispositions`
- <a id="s-f51aa17fd3"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-32390caf38"></a>`unit`: `member`

### Declared structure

- <a id="s-8bb37ba608"></a>`kind`: `"method"`
- <a id="s-0a8719445d"></a>`signature`: `"\"(self, claim_id: 'ProcessingClaimId', *, fence: 'int', dispositions: 'Sequence[DispositionInput]') -> 'ArtifactDispositionSetDocument'\""`

## Maintained corroboration

### Related interface records

- [PUT /v1/collection-processing-claims/{claim_id}/derivation/dispositions](../../riverhog/http-operations/put-v1-collection-processing-claims-claim-id-derivation-dispositions.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-134c5ee253"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/workflows.py::CollectionWorkflowMethods.record\_processing\_claim\_dispositions](../../../../../../packages/riverhog-client/src/riverhog_client/workflows.py#L493)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.record_processing_claim_dispositions`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 91061a7d90025b30af1b860551481f4d6400225a76a6541201fa8d88800b27b1 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'ProcessingClaimId', *, fence: 'int', dispositions: 'Sequence[DispositionInput]') -> 'ArtifactDispositionSetDocument'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "record_processing_claim_dispositions",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
