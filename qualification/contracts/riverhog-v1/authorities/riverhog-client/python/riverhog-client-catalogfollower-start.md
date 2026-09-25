# riverhog_client.CatalogFollower.start

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-catalogfollower-start:45d07c81ce -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8bab6425a5"></a>
- <a id="s-39781fe1d0"></a>`distribution`: `riverhog-client`
- <a id="s-3ea34d2077"></a>`module`: `riverhog_client`
- <a id="s-ec297eaf8d"></a>`name`: `start`
- <a id="s-77baa372a4"></a>`owner`: `riverhog_client.CatalogFollower`
- <a id="s-7c8f4a258b"></a>`unit`: `member`

### Declared structure

- <a id="s-28532b6e02"></a>`kind`: `"method"`
- <a id="s-cf004e61bd"></a>`signature`: `"\"(self, position: 'CatalogFollowPosition') -> 'CatalogFollowBatch'\""`

## Maintained corroboration

### Related interface records

- [CatalogFollower](riverhog-client-catalogfollower.md)

## Governing policies

- <a id="pa-e3851a6e0e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.CatalogFollower.start`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8c4e5932f3c320051b99e234ba5ac6d33a2c359cc05d5592377cced46c220e85 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, position: 'CatalogFollowPosition') -> 'CatalogFollowBatch'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "start",
  "owner": "riverhog_client.CatalogFollower",
  "unit": "member"
}
```

</details>
