# riverhog_client.ApiClient.append_processing_capability_artifacts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-append-processi-e128b5d422:8d6d48df91 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b727a36588"></a>
- <a id="s-8ee3e37891"></a>`distribution`: `riverhog-client`
- <a id="s-2846169fa5"></a>`module`: `riverhog_client`
- <a id="s-c374b6ac67"></a>`name`: `append_processing_capability_artifacts`
- <a id="s-ed968be049"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-3a41695511"></a>`unit`: `member`

### Declared structure

- <a id="s-014a6249f2"></a>`kind`: `"method"`
- <a id="s-73dcb011a4"></a>`signature`: `"\"(self, claim_id: 'ProcessingClaimId', capability_id: 'str', *, fence: 'int', start_ordinal: 'int', artifacts: 'Sequence[ArtifactInput]') -> 'ArtifactReceivingSetDocument'\""`

## Maintained corroboration

### Related interface records

- [PUT /v1/collection-processing-claims/{claim_id}/capabilities/{capability_id}/artifacts](../../riverhog/http-operations/put-v1-collection-processing-claims-claim-id-capabilities-capability-id-artifacts.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-ff0a8d23be"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/workflows.py::CollectionWorkflowMethods.append\_processing\_capability\_artifacts](../../../../../../packages/riverhog-client/src/riverhog_client/workflows.py#L441)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.append_processing_capability_artifacts`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6fd5740d94405fc386266fa77642c5fa3da4703106d1649086200713bef12e56 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'ProcessingClaimId', capability_id: 'str', *, fence: 'int', start_ordinal: 'int', artifacts: 'Sequence[ArtifactInput]') -> 'ArtifactReceivingSetDocument'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "append_processing_capability_artifacts",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
