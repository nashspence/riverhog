# riverhog_client.ApiClient.append_processing_claim_artifacts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-append-processi-4ddafa19e9:139b2adabc -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-39dab7f67a"></a>
- <a id="s-566c41a3df"></a>`distribution`: `riverhog-client`
- <a id="s-a83f544161"></a>`module`: `riverhog_client`
- <a id="s-b56f633bda"></a>`name`: `append_processing_claim_artifacts`
- <a id="s-9ff997b201"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-155b7254e8"></a>`unit`: `member`

### Declared structure

- <a id="s-e4e1babdeb"></a>`kind`: `"method"`
- <a id="s-8ba22f5fc2"></a>`signature`: `"\"(self, claim_id: 'ProcessingClaimId', *, fence: 'int', start_ordinal: 'int', artifacts: 'Sequence[ArtifactInput]') -> 'ArtifactReceivingSetDocument'\""`

## Maintained corroboration

### Related interface records

- [PUT /v1/collection-processing-claims/{claim_id}/plan/artifacts](../../riverhog/http-operations/put-v1-collection-processing-claims-claim-id-plan-artifacts.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-adabfed80d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/workflows.py::CollectionWorkflowMethods.append\_processing\_claim\_artifacts](../../../../../../packages/riverhog-client/src/riverhog_client/workflows.py#L340)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.append_processing_claim_artifacts`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3f41d6ea3de1cf31fdd2f4973c7edaca26e74b87ebb514e2a0afd5aca94ca10c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'ProcessingClaimId', *, fence: 'int', start_ordinal: 'int', artifacts: 'Sequence[ArtifactInput]') -> 'ArtifactReceivingSetDocument'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "append_processing_claim_artifacts",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
