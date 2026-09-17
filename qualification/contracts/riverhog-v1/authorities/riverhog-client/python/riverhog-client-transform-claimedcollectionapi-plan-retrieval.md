# riverhog_client.transform.ClaimedCollectionApi.plan_retrieval

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-claimedcollecti-a832a7d561:8878b9e1d7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4b16e8bf3d"></a>
- <a id="s-2c68d45a54"></a>`distribution`: `riverhog-client`
- <a id="s-8dde36a327"></a>`module`: `riverhog_client.transform`
- <a id="s-f054703da9"></a>`name`: `plan_retrieval`
- <a id="s-1c2e96e4b7"></a>`owner`: `riverhog_client.transform.ClaimedCollectionApi`
- <a id="s-248bd1c1fc"></a>`unit`: `member`

### Declared structure

- <a id="s-bfa4d98ef3"></a>`kind`: `"method"`
- <a id="s-52a4b559e4"></a>`signature`: `"\"(self, files: 'Sequence[tuple[CollectionId, str]]', *, lease_seconds: 'int \| None' = None, restore_policy: 'RiverhogRestorePolicy' = 'never') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [ClaimedCollectionApi](riverhog-client-transform-claimedcollectionapi.md)

## Governing policies

- <a id="pa-63104ea1a7"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources/authorities.md#src-7a247bb534) — [packages/riverhog-client/src/riverhog\_client/transform/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/transform/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.transform.ClaimedCollectionApi.plan_retrieval`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 45c9e46d918bc352b19c6eb255614fd67c6979798c2844260a343e2b5f368e81 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, files: 'Sequence[tuple[CollectionId, str]]', *, lease_seconds: 'int | None' = None, restore_policy: 'RiverhogRestorePolicy' = 'never') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "plan_retrieval",
  "owner": "riverhog_client.transform.ClaimedCollectionApi",
  "unit": "member"
}
```

</details>
