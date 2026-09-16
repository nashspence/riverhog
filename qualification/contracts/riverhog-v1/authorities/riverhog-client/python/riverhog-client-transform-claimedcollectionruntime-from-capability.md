# riverhog_client.transform.ClaimedCollectionRuntime.from_capability

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-claimedcollecti-f662e9e9b9:9158f5b916 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-094fedb532"></a>
- <a id="s-980501278b"></a>`distribution`: `riverhog-client`
- <a id="s-14b887b058"></a>`module`: `riverhog_client.transform`
- <a id="s-5ab0863f9e"></a>`name`: `from_capability`
- <a id="s-07509f9823"></a>`owner`: `riverhog_client.transform.ClaimedCollectionRuntime`
- <a id="s-f25c4774dd"></a>`unit`: `member`

### Declared structure

- <a id="s-27e27141e9"></a>`kind`: `"classmethod"`
- <a id="s-bffcbea85f"></a>`signature`: `"\"(cls, *, base_url: 'str', capability_token: 'str', inputs: 'Sequence[CollectionRootIdentity]', claim_id: 'str', fence: 'int', work_id: 'str', execution_id: 'str', allow_insecure_http: 'bool' = False, **kwargs: 'Any') -> 'ClaimedCollectionRuntime'\""`

## Maintained corroboration

### Related interface records

- [ClaimedCollectionRuntime](riverhog-client-transform-claimedcollectionruntime.md)

## Governing policies

- <a id="pa-0faff9dd49"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.ClaimedCollectionRuntime.from_capability`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9edb888e636b75536eb85d45480634c5b6ca1c639c18b6bbc7f0af220a828daf -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, *, base_url: 'str', capability_token: 'str', inputs: 'Sequence[CollectionRootIdentity]', claim_id: 'str', fence: 'int', work_id: 'str', execution_id: 'str', allow_insecure_http: 'bool' = False, **kwargs: 'Any') -> 'ClaimedCollectionRuntime'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "from_capability",
  "owner": "riverhog_client.transform.ClaimedCollectionRuntime",
  "unit": "member"
}
```

</details>
