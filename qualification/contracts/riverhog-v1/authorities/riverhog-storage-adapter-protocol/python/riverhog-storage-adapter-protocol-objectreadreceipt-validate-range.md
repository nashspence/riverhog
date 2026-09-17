# riverhog_storage_adapter_protocol.ObjectReadReceipt.validate_range

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-objectr-0458d2972c:5581e91af8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5552263b2e"></a>
- <a id="s-2ea79ad615"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-c658df97cd"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-43edf1b93b"></a>`name`: `validate_range`
- <a id="s-b13af3e560"></a>`owner`: `riverhog_storage_adapter_protocol.ObjectReadReceipt`
- <a id="s-c61621ea67"></a>`unit`: `member`

### Declared structure

- <a id="s-939e7fea73"></a>`kind`: `"method"`
- <a id="s-36dec29a34"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [ObjectReadReceipt](riverhog-storage-adapter-protocol-objectreadreceipt.md)

## Governing policies

- <a id="pa-33a9964a22"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources/authorities.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ObjectReadReceipt.validate_range`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ada8061a4428aef19215b5065766204d6b83389898ed51b18ca40e22875da33e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "validate_range",
  "owner": "riverhog_storage_adapter_protocol.ObjectReadReceipt",
  "unit": "member"
}
```

</details>
