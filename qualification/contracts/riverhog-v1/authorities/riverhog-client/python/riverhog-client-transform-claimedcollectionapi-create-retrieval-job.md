# riverhog_client.transform.ClaimedCollectionApi.create_retrieval_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-claimedcollecti-ac0bd6be57:5e040aebfb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0cf31d5419"></a>
- <a id="s-a67aa2608d"></a>`distribution`: `riverhog-client`
- <a id="s-66df27a2c9"></a>`module`: `riverhog_client.transform`
- <a id="s-326e179cc0"></a>`name`: `create_retrieval_job`
- <a id="s-e137f04092"></a>`owner`: `riverhog_client.transform.ClaimedCollectionApi`
- <a id="s-50b98537b1"></a>`unit`: `member`

### Declared structure

- <a id="s-f83bf5da5c"></a>`kind`: `"method"`
- <a id="s-4486284d4e"></a>`signature`: `"\"(self, plan_id: 'str', *, plan_etag: 'str', event_context: 'Mapping[str, Any] \| None' = None) -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [ClaimedCollectionApi](riverhog-client-transform-claimedcollectionapi.md)

## Governing policies

- <a id="pa-1e9511a49e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources/authorities.md#src-7a247bb534) — [packages/riverhog-client/src/riverhog\_client/transform/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/transform/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.transform.ClaimedCollectionApi.create_retrieval_job`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b3a13290015d8c57e83e225dab1c07d3ac98d189069b5787263900e68ecf03fb -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, plan_id: 'str', *, plan_etag: 'str', event_context: 'Mapping[str, Any] | None' = None) -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "create_retrieval_job",
  "owner": "riverhog_client.transform.ClaimedCollectionApi",
  "unit": "member"
}
```

</details>
