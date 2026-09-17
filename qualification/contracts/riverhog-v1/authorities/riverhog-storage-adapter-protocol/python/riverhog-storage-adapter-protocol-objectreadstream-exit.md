# riverhog_storage_adapter_protocol.ObjectReadStream.__exit__

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-objectr-7658e58a78:d768cf399f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c43149f9bb"></a>
- <a id="s-a765a03484"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-78245b6e4e"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-d1224c5d20"></a>`name`: `__exit__`
- <a id="s-fcf59734aa"></a>`owner`: `riverhog_storage_adapter_protocol.ObjectReadStream`
- <a id="s-7f8a619b52"></a>`unit`: `member`

### Declared structure

- <a id="s-8286d11b0f"></a>`kind`: `"method"`
- <a id="s-5e45e3fe5a"></a>`signature`: `"\"(self, *_: 'object') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [ObjectReadStream](riverhog-storage-adapter-protocol-objectreadstream.md)

## Governing policies

- <a id="pa-c623b9e18f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources/authorities.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ObjectReadStream.__exit__`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f293a042d3ba62ebe2e59ea25b93507e6957ae321fcf67d1280a872058c353fd -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *_: 'object') -> 'None'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "__exit__",
  "owner": "riverhog_storage_adapter_protocol.ObjectReadStream",
  "unit": "member"
}
```

</details>
