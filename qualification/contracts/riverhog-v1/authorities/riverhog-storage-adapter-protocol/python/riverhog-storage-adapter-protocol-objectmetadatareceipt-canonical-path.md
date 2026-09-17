# riverhog_storage_adapter_protocol.ObjectMetadataReceipt.canonical_path

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-objectm-e2e6c63d4a:507796a959 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6db44f897a"></a>
- <a id="s-24ed6616cd"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-74edb8db89"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-2574d17bd8"></a>`name`: `canonical_path`
- <a id="s-994254e703"></a>`owner`: `riverhog_storage_adapter_protocol.ObjectMetadataReceipt`
- <a id="s-b4fbea3237"></a>`unit`: `member`

### Declared structure

- <a id="s-0d9497256d"></a>`kind`: `"classmethod"`
- <a id="s-34e80b05ae"></a>`signature`: `"\"(cls, value: 'str') -> 'str'\""`

## Maintained corroboration

### Related interface records

- [ObjectMetadataReceipt](riverhog-storage-adapter-protocol-objectmetadatareceipt.md)

## Governing policies

- <a id="pa-fb1a72a974"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources/authorities.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ObjectMetadataReceipt.canonical_path`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 374ea22fa0f8bdb75abcc85026931faca94ac241a557227e2cf9c856e3d35b31 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'str') -> 'str'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "canonical_path",
  "owner": "riverhog_storage_adapter_protocol.ObjectMetadataReceipt",
  "unit": "member"
}
```

</details>
