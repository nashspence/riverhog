# riverhog_client.ApiClient.seal_processing_capability_artifacts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-seal-processing-a05ed504a8:c24a73bae3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d71831ce75"></a>
- <a id="s-cf764e0d98"></a>`distribution`: `riverhog-client`
- <a id="s-e92b1cec21"></a>`module`: `riverhog_client`
- <a id="s-d1694fd276"></a>`name`: `seal_processing_capability_artifacts`
- <a id="s-d5c521667d"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-ca8814cbc2"></a>`unit`: `member`

### Declared structure

- <a id="s-3ec653598f"></a>`kind`: `"method"`
- <a id="s-9c3fc65f0b"></a>`signature`: `"\"(self, claim_id: 'ProcessingClaimId', capability_id: 'str', *, fence: 'int') -> 'ArtifactReceivingSetDocument'\""`

## Maintained corroboration

### Related interface records

- [POST /v1/collection-processing-claims/{claim_id}/capabilities/{capability_id}/artifacts/seal](../../riverhog/http-operations/post-v1-collection-processing-claims-claim-id-capabilities-capability-id-artifacts-seal.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-236ead50c4"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/workflows.py::CollectionWorkflowMethods.seal\_processing\_capability\_artifacts](../../../../../../packages/riverhog-client/src/riverhog_client/workflows.py#L468)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.seal_processing_capability_artifacts`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dfe999704dbb12692a9d8bda4f1efb52169eed9345277ca6abbdc0ba1e1f1654 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'ProcessingClaimId', capability_id: 'str', *, fence: 'int') -> 'ArtifactReceivingSetDocument'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "seal_processing_capability_artifacts",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
