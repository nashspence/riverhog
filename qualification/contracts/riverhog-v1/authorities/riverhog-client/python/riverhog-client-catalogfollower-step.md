# riverhog_client.CatalogFollower.step

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-catalogfollower-step:13480a0b3d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b16ee7b3b3"></a>
- <a id="s-4b4fc9bdd5"></a>`distribution`: `riverhog-client`
- <a id="s-74ab89fe8e"></a>`module`: `riverhog_client`
- <a id="s-3fbf82044d"></a>`name`: `step`
- <a id="s-91796ceedf"></a>`owner`: `riverhog_client.CatalogFollower`
- <a id="s-7cecdcd05d"></a>`unit`: `member`

### Declared structure

- <a id="s-c3ea9e4fbf"></a>`kind`: `"method"`
- <a id="s-836a3d723c"></a>`signature`: `"\"(self, position: 'CatalogFollowPosition', *, limit: 'int' = 100) -> 'CatalogFollowBatch'\""`

## Maintained corroboration

### Related interface records

- [CatalogFollower](riverhog-client-catalogfollower.md)

## Governing policies

- <a id="pa-878b6b9814"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.CatalogFollower.step`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 99272c629fcc96e6e26da8270e249f0dbc3e6a5038f2c50fce9dcb7d2b1bde8c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, position: 'CatalogFollowPosition', *, limit: 'int' = 100) -> 'CatalogFollowBatch'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "step",
  "owner": "riverhog_client.CatalogFollower",
  "unit": "member"
}
```

</details>
