# riverhog_client.processing.ClaimedCollectionRuntime.iter_inventory

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-claimedcollect-51ff944b45:a357459b37 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-133394e04a"></a>
- <a id="s-867201a3bb"></a>`distribution`: `riverhog-client`
- <a id="s-0f939daab6"></a>`module`: `riverhog_client.processing`
- <a id="s-f5db4a775e"></a>`name`: `iter_inventory`
- <a id="s-aa3c0388f2"></a>`owner`: `riverhog_client.processing.ClaimedCollectionRuntime`
- <a id="s-bd76788f59"></a>`unit`: `member`

### Declared structure

- <a id="s-33818da96c"></a>`kind`: `"method"`
- <a id="s-43055fd73c"></a>`signature`: `"'(self)'"`

## Maintained corroboration

### Related interface records

- [ClaimedCollectionRuntime](riverhog-client-processing-claimedcollectionruntime.md)

## Governing policies

- <a id="pa-2ab41a88bf"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.ClaimedCollectionRuntime.iter_inventory`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 715bdbc5be9fa1ca6eea1f8c9bc3a1f54cfaf3b256872a5b442c0b008399fb1b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "'(self)'"
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "iter_inventory",
  "owner": "riverhog_client.processing.ClaimedCollectionRuntime",
  "unit": "member"
}
```

</details>
