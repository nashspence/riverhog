# riverhog_protocol.ArchiveCopyJobSort

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-archivecopyjobsort:302584a60d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4337609cc3"></a>
- <a id="s-fec8d9b3b8"></a>`distribution`: `riverhog-protocol`
- <a id="s-b878b72bd5"></a>`module`: `riverhog_protocol`
- <a id="s-f81f8b2c53"></a>`name`: `ArchiveCopyJobSort`
- <a id="s-d7768ba1f2"></a>`unit`: `export`

### Declared structure

- <a id="s-4d83330453"></a>`kind`: `"type-alias"`
- <a id="s-ac98cb3690"></a>`value`: `"typing.Literal['collection_id', 'source_store', 'destination_store', 'state', 'requested_at']"`

## Governing policies

- <a id="pa-472c276683"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.ArchiveCopyJobSort`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: af63a96be41c3758a5739a33e18f8313a1575efff9c7f9645427252efa59175c -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Literal['collection_id', 'source_store', 'destination_store', 'state', 'requested_at']"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ArchiveCopyJobSort",
  "unit": "export"
}
```

</details>
