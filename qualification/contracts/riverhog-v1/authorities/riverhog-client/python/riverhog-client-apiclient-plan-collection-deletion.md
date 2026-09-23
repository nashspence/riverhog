# riverhog_client.ApiClient.plan_collection_deletion

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-plan-collection-deletion:1cdb86785e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2383a8690b"></a>
- <a id="s-61b2579d58"></a>`distribution`: `riverhog-client`
- <a id="s-f555182716"></a>`module`: `riverhog_client`
- <a id="s-837d1f843b"></a>`name`: `plan_collection_deletion`
- <a id="s-354a532ca8"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-b43dd212a1"></a>`unit`: `member`

### Declared structure

- <a id="s-3a4af06192"></a>`kind`: `"method"`
- <a id="s-61fa4ae3ec"></a>`signature`: `"\"(self, collection_id: 'CollectionId', *, retirement_claim_id: 'ProcessingClaimId \| None' = None) -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [a-riverhog-cli collection delete](../../a-riverhog-cli/cli/a-riverhog-cli-collection-delete.md)
- [POST /v1/collections/{collection_id}/deletion-plan](../../riverhog/http-operations/post-v1-collections-collection-id-deletion-plan.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-7f47a3e1be"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.plan\_collection\_deletion](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L1967)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.plan_collection_deletion`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 913ff682b8ec084d82df0c19f25bce9e78d0eac22cdb93ebb7a5ab2b7f3abde8 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', *, retirement_claim_id: 'ProcessingClaimId | None' = None) -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "plan_collection_deletion",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
