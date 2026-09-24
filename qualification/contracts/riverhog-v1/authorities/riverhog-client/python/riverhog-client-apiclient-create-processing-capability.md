# riverhog_client.ApiClient.create_processing_capability

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-create-processi-d54c0d635b:3ac202076c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-67d9c862d8"></a>
- <a id="s-d11318d2ca"></a>`distribution`: `riverhog-client`
- <a id="s-1cc897fc65"></a>`module`: `riverhog_client`
- <a id="s-e146214480"></a>`name`: `create_processing_capability`
- <a id="s-8039f7c30e"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-b3d00db8b9"></a>`unit`: `member`

### Declared structure

- <a id="s-a22b1a9c3d"></a>`kind`: `"method"`
- <a id="s-b470f4c01d"></a>`signature`: `"\"(self, claim_id: 'ProcessingClaimId', *, fence: 'int', audience: 'str', actions: 'Sequence[CapabilityAction]' = ('read-inputs',), artifacts: 'Iterable[ArtifactInput]', ttl_seconds: 'int' = 900) -> 'ProcessingCapabilityDocument'\""`

## Maintained corroboration

### Related interface records

- [POST /v1/collection-processing-claims/{claim_id}/capabilities](../../riverhog/http-operations/post-v1-collection-processing-claims-claim-id-capabilities.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-e4c52bc341"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/workflows.py::CollectionWorkflowMethods.create\_processing\_capability](../../../../../../packages/riverhog-client/src/riverhog_client/workflows.py#L399)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.create_processing_capability`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b440a493ad814bff552c1530f317e7d8c1d2dbab127c43a6424c61fc703be1fc -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'ProcessingClaimId', *, fence: 'int', audience: 'str', actions: 'Sequence[CapabilityAction]' = ('read-inputs',), artifacts: 'Iterable[ArtifactInput]', ttl_seconds: 'int' = 900) -> 'ProcessingCapabilityDocument'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "create_processing_capability",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
