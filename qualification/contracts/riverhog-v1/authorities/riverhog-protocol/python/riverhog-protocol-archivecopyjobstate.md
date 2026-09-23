# riverhog_protocol.ArchiveCopyJobState

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-archivecopyjobstate:ad9663a50d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-65fdf1fa71"></a>
- <a id="s-117b98dd9e"></a>`distribution`: `riverhog-protocol`
- <a id="s-2b8ccdb08c"></a>`module`: `riverhog_protocol`
- <a id="s-36501c6a32"></a>`name`: `ArchiveCopyJobState`
- <a id="s-e8b0947b05"></a>`unit`: `export`

### Declared structure

- <a id="s-4b8b4ae9c1"></a>`kind`: `"type-alias"`
- <a id="s-15616eacff"></a>`value`: `"typing.Literal['requested', 'waiting', 'checking', 'copying', 'canceling', 'completed', 'failed', 'canceled']"`

## Governing policies

- <a id="pa-f30eaf42b6"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.ArchiveCopyJobState`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2088fc06c67afad13bed5921e19072ef311b90e6993b5be9dbbb8744e7bcaf4e -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Literal['requested', 'waiting', 'checking', 'copying', 'canceling', 'completed', 'failed', 'canceled']"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ArchiveCopyJobState",
  "unit": "export"
}
```

</details>
