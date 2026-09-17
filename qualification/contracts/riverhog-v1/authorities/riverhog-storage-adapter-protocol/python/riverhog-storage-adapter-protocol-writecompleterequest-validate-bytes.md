# riverhog_storage_adapter_protocol.WriteCompleteRequest.validate_bytes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-writeco-a0224de961:ea521185da -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-93cca69db4"></a>
- <a id="s-52b61fc5f9"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-6552e7e25c"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-96fa32a7f6"></a>`name`: `validate_bytes`
- <a id="s-f34ba856a2"></a>`owner`: `riverhog_storage_adapter_protocol.WriteCompleteRequest`
- <a id="s-1471111c04"></a>`unit`: `member`

### Declared structure

- <a id="s-8f114fd3c2"></a>`kind`: `"method"`
- <a id="s-8158244fb3"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [WriteCompleteRequest](riverhog-storage-adapter-protocol-writecompleterequest.md)

## Governing policies

- <a id="pa-28e093c743"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources/authorities.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.WriteCompleteRequest.validate_bytes`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cea679a91a5e8103850a9fe848b944de83032796a2059dd8f703bcc1816983db -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "validate_bytes",
  "owner": "riverhog_storage_adapter_protocol.WriteCompleteRequest",
  "unit": "member"
}
```

</details>
