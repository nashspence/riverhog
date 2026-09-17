# riverhog_protocol.ArchiveStoreSort

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-archivestoresort:a5e40b3f07 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d7c84627dc"></a>
- <a id="s-666c18d676"></a>`distribution`: `riverhog-protocol`
- <a id="s-ac892897a3"></a>`module`: `riverhog_protocol`
- <a id="s-11587834eb"></a>`name`: `ArchiveStoreSort`
- <a id="s-d3525e3dfd"></a>`unit`: `export`

### Declared structure

- <a id="s-1ebe33412f"></a>`kind`: `"type-alias"`
- <a id="s-43233a5b6a"></a>`value`: `"typing.Literal['store', 'read_mode', 'read_priority', 'collections', 'objects', 'stored_bytes']"`

## Governing policies

- <a id="pa-380b52c51e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.ArchiveStoreSort`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1d0b4fa78c2b55080286d598c44625b1f56fff7ce0fc9b38a475bc0bdbce62f2 -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Literal['store', 'read_mode', 'read_priority', 'collections', 'objects', 'stored_bytes']"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ArchiveStoreSort",
  "unit": "export"
}
```

</details>
