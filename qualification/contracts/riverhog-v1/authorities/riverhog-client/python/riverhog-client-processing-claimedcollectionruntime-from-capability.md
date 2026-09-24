# riverhog_client.processing.ClaimedCollectionRuntime.from_capability

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-claimedcollect-51e6799f66:3126fd3c5a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-669a309c54"></a>
- <a id="s-1ef9601b10"></a>`distribution`: `riverhog-client`
- <a id="s-8f1ecc1496"></a>`module`: `riverhog_client.processing`
- <a id="s-42331ae6df"></a>`name`: `from_capability`
- <a id="s-6e26cc4fe1"></a>`owner`: `riverhog_client.processing.ClaimedCollectionRuntime`
- <a id="s-62d19eb138"></a>`unit`: `member`

### Declared structure

- <a id="s-2b2f1c8af8"></a>`kind`: `"classmethod"`
- <a id="s-9296787f2b"></a>`signature`: `"\"(cls, *, base_url: 'str', capability_token: 'str', inputs: 'Sequence[CollectionRootIdentity]', claim_id: 'str', fence: 'int', work_id: 'str', execution_id: 'str', allow_insecure_http: 'bool' = False, **kwargs: 'Any') -> 'ClaimedCollectionRuntime'\""`

## Maintained corroboration

### Related interface records

- [ClaimedCollectionRuntime](riverhog-client-processing-claimedcollectionruntime.md)

## Governing policies

- <a id="pa-39e9fddd8f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.ClaimedCollectionRuntime.from_capability`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6fba67433cab98383d63866417044d1d9273760cd3a68b9de2d35829dcfc3aeb -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, *, base_url: 'str', capability_token: 'str', inputs: 'Sequence[CollectionRootIdentity]', claim_id: 'str', fence: 'int', work_id: 'str', execution_id: 'str', allow_insecure_http: 'bool' = False, **kwargs: 'Any') -> 'ClaimedCollectionRuntime'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "from_capability",
  "owner": "riverhog_client.processing.ClaimedCollectionRuntime",
  "unit": "member"
}
```

</details>
