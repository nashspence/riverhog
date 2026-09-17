# riverhog_protocol.COLLECTION_UPLOAD_FILE_BATCH_MAX

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collection-upload-file-batch-max:da11a6823c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e2635c93fd"></a>
- <a id="s-b6a27131f3"></a>`distribution`: `riverhog-protocol`
- <a id="s-01ab7daf38"></a>`module`: `riverhog_protocol`
- <a id="s-811f2229eb"></a>`name`: `COLLECTION_UPLOAD_FILE_BATCH_MAX`
- <a id="s-c78ca06a31"></a>`unit`: `export`

### Declared structure

- <a id="s-802582d596"></a>`kind`: `"constant"`
- <a id="s-8c86b48d43"></a>`value`: `100`

## Governing policies

- <a id="pa-6627425dab"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.COLLECTION_UPLOAD_FILE_BATCH_MAX`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c1a12590c98a303b4fdf5c80a8a0832f552a7db29223352c8df3772a0d90c0bd -->

```json
{
  "contract": {
    "kind": "constant",
    "value": 100
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "COLLECTION_UPLOAD_FILE_BATCH_MAX",
  "unit": "export"
}
```

</details>
