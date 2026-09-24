# riverhog_client.processing.ClaimedCollectionApi.plan_retrieval

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-claimedcollect-7d10d57455:926357266f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-36ad3c01c2"></a>
- <a id="s-dc3b4dba5c"></a>`distribution`: `riverhog-client`
- <a id="s-f4875bf780"></a>`module`: `riverhog_client.processing`
- <a id="s-d45b73b8d0"></a>`name`: `plan_retrieval`
- <a id="s-640cd983d6"></a>`owner`: `riverhog_client.processing.ClaimedCollectionApi`
- <a id="s-25124f18c6"></a>`unit`: `member`

### Declared structure

- <a id="s-39e0568fe9"></a>`kind`: `"method"`
- <a id="s-22157ddcd6"></a>`signature`: `"\"(self, files: 'Sequence[tuple[CollectionId, str]]', *, lease_seconds: 'int \| None' = None, restore_policy: 'RiverhogRestorePolicy' = 'never') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [ClaimedCollectionApi](riverhog-client-processing-claimedcollectionapi.md)

## Governing policies

- <a id="pa-c793414e7c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.ClaimedCollectionApi.plan_retrieval`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1cf73cda209b49ac075b6cc79fda44a56bb1a160b1419c2548881d56e7fc90c3 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, files: 'Sequence[tuple[CollectionId, str]]', *, lease_seconds: 'int | None' = None, restore_policy: 'RiverhogRestorePolicy' = 'never') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "plan_retrieval",
  "owner": "riverhog_client.processing.ClaimedCollectionApi",
  "unit": "member"
}
```

</details>
