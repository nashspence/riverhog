# riverhog_client.transform.ClaimedCollectionApi.list_retrieval_plan_files

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-claimedcollecti-a523355fe2:ef265f9274 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-deb82b7f81"></a>
- <a id="s-456c6f21f3"></a>`distribution`: `riverhog-client`
- <a id="s-78bb78c3d2"></a>`module`: `riverhog_client.transform`
- <a id="s-385c55ba93"></a>`name`: `list_retrieval_plan_files`
- <a id="s-321759f2eb"></a>`owner`: `riverhog_client.transform.ClaimedCollectionApi`
- <a id="s-5ff88ab691"></a>`unit`: `member`

### Declared structure

- <a id="s-bf212f5a59"></a>`kind`: `"method"`
- <a id="s-45173796a8"></a>`signature`: `"\"(self, plan_id: 'str', *, plan_etag: 'str', start_ordinal: 'int' = 0, page_size: 'int' = 100) -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [ClaimedCollectionApi](riverhog-client-transform-claimedcollectionapi.md)

## Governing policies

- <a id="pa-e2f3290e80"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.ClaimedCollectionApi.list_retrieval_plan_files`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 63f525c67873cae9717bbdb7ba3f3d9c8b04aaa5d38d3e4d91e8e2611d748b17 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, plan_id: 'str', *, plan_etag: 'str', start_ordinal: 'int' = 0, page_size: 'int' = 100) -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "list_retrieval_plan_files",
  "owner": "riverhog_client.transform.ClaimedCollectionApi",
  "unit": "member"
}
```

</details>
