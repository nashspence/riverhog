# riverhog_protocol.ArchiveCopySort

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-archivecopysort:9b2108fbaf -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-775f00d237"></a>
- <a id="s-7574f08b90"></a>`distribution`: `riverhog-protocol`
- <a id="s-98c421fb97"></a>`module`: `riverhog_protocol`
- <a id="s-2200239718"></a>`name`: `ArchiveCopySort`
- <a id="s-600285705a"></a>`unit`: `export`

### Declared structure

- <a id="s-95edcab209"></a>`kind`: `"type-alias"`
- <a id="s-7675588d88"></a>`value`: `"typing.Literal['collection_id', 'source_store', 'destination_store', 'state', 'requested_at']"`

## Governing policies

- <a id="pa-58f3a11b61"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.ArchiveCopySort`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bbdf4393729b3039b7cb219e6b7621900bf86b5370efef6a67c2ca0f3c3ea2dd -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Literal['collection_id', 'source_store', 'destination_store', 'state', 'requested_at']"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ArchiveCopySort",
  "unit": "export"
}
```

</details>
