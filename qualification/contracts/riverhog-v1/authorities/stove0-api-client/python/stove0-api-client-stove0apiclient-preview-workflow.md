# stove0_api_client.Stove0ApiClient.preview_workflow

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-api-client:stove0-api-client-stove0apiclient-preview-workflow:bba91bf18d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f4c1a334de"></a>
- <a id="s-67bafc385e"></a>`distribution`: `stove0-api-client`
- <a id="s-0136f37fb9"></a>`module`: `stove0_api_client`
- <a id="s-a6bb854228"></a>`name`: `preview_workflow`
- <a id="s-e90dc8939b"></a>`owner`: `stove0_api_client.Stove0ApiClient`
- <a id="s-d6ec9da5ef"></a>`unit`: `member`

### Declared structure

- <a id="s-9344ce3524"></a>`kind`: `"method"`
- <a id="s-fbfa2bf98d"></a>`signature`: `"\"(self, recipe_id: 'str', inputs: 'Sequence[CollectionRootRef]', *, recipe_revision: 'int \| None' = None, effective_intent: 'Mapping[str, Any] \| None' = None) -> 'WorkflowPreview'\""`

## Maintained corroboration

### Related interface records

- [stove0 preview](../../stove0-client/cli/stove0-preview.md)
- [POST /v1/workflow-previews](../../stove0/http-operations/post-v1-workflow-previews.md)
- [Stove0ApiClient](stove0-api-client-stove0apiclient.md)

## Governing policies

- <a id="pa-e0589d8aec"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-api-client:stove0_api_client](../../../evidence/sources.md#src-5d52ac5998) — `reference/stove0/packages/api-client/src/stove0_api_client/__init__.py`
- **Client method:** [reference/stove0/packages/api-client/src/stove0_api_client/client.py::Stove0ApiClient.preview_workflow](../../../../../../reference/stove0/packages/api-client/src/stove0_api_client/client.py#L324)

### Machine authority

- `/external_contract/python/stove0_api_client.Stove0ApiClient.preview_workflow`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 84bfc31ca84faf34f8cdb6d162dfc5b6d54efaa54e9ca8df4c718e870bfde97e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, recipe_id: 'str', inputs: 'Sequence[CollectionRootRef]', *, recipe_revision: 'int | None' = None, effective_intent: 'Mapping[str, Any] | None' = None) -> 'WorkflowPreview'\""
  },
  "distribution": "stove0-api-client",
  "module": "stove0_api_client",
  "name": "preview_workflow",
  "owner": "stove0_api_client.Stove0ApiClient",
  "unit": "member"
}
```

</details>
