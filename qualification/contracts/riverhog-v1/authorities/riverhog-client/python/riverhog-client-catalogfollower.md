# riverhog_client.CatalogFollower

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-catalogfollower:e28c7e13b7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f37e9e369c"></a>
- <a id="s-667ba110be"></a>`distribution`: `riverhog-client`
- <a id="s-b8ae44a3c2"></a>`module`: `riverhog_client`
- <a id="s-35b398b3ae"></a>`name`: `CatalogFollower`
- <a id="s-324b523e85"></a>`unit`: `export`

### Declared structure

- <a id="s-21fd92915e"></a>`kind`: `"class"`
- <a id="s-e06ec83c89"></a>`signature`: `"\"(api: 'CatalogFollowApi') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [start](riverhog-client-catalogfollower-start.md)
- [step](riverhog-client-catalogfollower-step.md)

## Governing policies

- <a id="pa-70f8d5d636"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.CatalogFollower`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 917026eb48ebdd7ddce2db4702e5fe78e6a63406f9b1f24056bb5da1e0be0539 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(api: 'CatalogFollowApi') -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "CatalogFollower",
  "unit": "export"
}
```

</details>
