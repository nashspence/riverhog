# riverhog_client.processing.ClaimedCollectionRuntime.close

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-claimedcollect-d7f0e8ee79:c92687bf5e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7a1363a1eb"></a>
- <a id="s-12eb1c2176"></a>`distribution`: `riverhog-client`
- <a id="s-20fd6b284e"></a>`module`: `riverhog_client.processing`
- <a id="s-a062b7981a"></a>`name`: `close`
- <a id="s-a94aebc5c7"></a>`owner`: `riverhog_client.processing.ClaimedCollectionRuntime`
- <a id="s-eb40477872"></a>`unit`: `member`

### Declared structure

- <a id="s-98dadec9f0"></a>`kind`: `"method"`
- <a id="s-72ce473f95"></a>`signature`: `"\"(self) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [ClaimedCollectionRuntime](riverhog-client-processing-claimedcollectionruntime.md)

## Governing policies

- <a id="pa-530c8e1b71"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.ClaimedCollectionRuntime.close`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c05894d468dd31e16286e6c0a2f0f095a91200ecbc9a2c14fc96e620f6ba6e9d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "close",
  "owner": "riverhog_client.processing.ClaimedCollectionRuntime",
  "unit": "member"
}
```

</details>
