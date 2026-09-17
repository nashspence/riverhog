# riverhog_storage_adapter_protocol.MAX_WRITE_SEGMENT_PAGE_ITEMS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-max-wri-7fccedd422:ed30dba24b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-026f924c06"></a>
- <a id="s-1ca300e18f"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-594bed72a6"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-6e096f2ae6"></a>`name`: `MAX_WRITE_SEGMENT_PAGE_ITEMS`
- <a id="s-37c8844e62"></a>`unit`: `export`

### Declared structure

- <a id="s-a30b8b9971"></a>`kind`: `"constant"`
- <a id="s-e8a7938dad"></a>`value`: `128`

## Governing policies

- <a id="pa-a068441208"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources/authorities.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.MAX_WRITE_SEGMENT_PAGE_ITEMS`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9cc82debd60d853c108fa5113d277a38622f977d5a8f67fb0b070393c42b36b9 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": 128
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "MAX_WRITE_SEGMENT_PAGE_ITEMS",
  "unit": "export"
}
```

</details>
