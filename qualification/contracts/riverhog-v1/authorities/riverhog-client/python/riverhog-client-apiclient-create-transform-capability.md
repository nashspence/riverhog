# riverhog_client.ApiClient.create_transform_capability

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-create-transfor-233a5f5866:7a4fb65a0b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-24a111a30b"></a>
- <a id="s-9eef18cc34"></a>`distribution`: `riverhog-client`
- <a id="s-3c896f5de0"></a>`module`: `riverhog_client`
- <a id="s-fe6bcf6b3d"></a>`name`: `create_transform_capability`
- <a id="s-1532fa8fb8"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-1a3dc8f285"></a>`unit`: `member`

### Declared structure

- <a id="s-5657f259b7"></a>`kind`: `"method"`
- <a id="s-425d04bea3"></a>`signature`: `"\"(self, claim_id: 'ProcessingClaimId', *, fence: 'int', audience: 'str', actions: 'Sequence[CapabilityAction]' = ('read-inputs',), artifacts: 'Iterable[ArtifactInput]', ttl_seconds: 'int' = 900) -> 'TransformCapabilityDocument'\""`

## Maintained corroboration

### Related interface records

- [POST /v1/collection-processing-claims/{claim_id}/capabilities](../../riverhog/http-operations/post-v1-collection-processing-claims-claim-id-capabilities.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-37f91cc55e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/workflows.py::CollectionWorkflowMethods.create\_transform\_capability](../../../../../../packages/riverhog-client/src/riverhog_client/workflows.py#L399)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.create_transform_capability`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7fb3ea65225b6618a1698040c04ebae84a0641f3c4348f4924c4b54d8c24efff -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'ProcessingClaimId', *, fence: 'int', audience: 'str', actions: 'Sequence[CapabilityAction]' = ('read-inputs',), artifacts: 'Iterable[ArtifactInput]', ttl_seconds: 'int' = 900) -> 'TransformCapabilityDocument'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "create_transform_capability",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
