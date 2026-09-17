# riverhog_protocol.ArchiveCopyState

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-archivecopystate:ee49f8608f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3128cda516"></a>
- <a id="s-4cae8e37a0"></a>`distribution`: `riverhog-protocol`
- <a id="s-3258dbfb0d"></a>`module`: `riverhog_protocol`
- <a id="s-b62aa1aa56"></a>`name`: `ArchiveCopyState`
- <a id="s-b9ebdec850"></a>`unit`: `export`

### Declared structure

- <a id="s-5a4210f67f"></a>`kind`: `"type-alias"`
- <a id="s-461016b51b"></a>`value`: `"typing.Literal['requested', 'waiting', 'checking', 'copying', 'canceling', 'completed', 'failed', 'canceled']"`

## Governing policies

- <a id="pa-579d82eb78"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.ArchiveCopyState`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f45c9c9a11c195a74fe39bc11218a4049e96237991cfb9c01a55f8094477880e -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Literal['requested', 'waiting', 'checking', 'copying', 'canceling', 'completed', 'failed', 'canceled']"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ArchiveCopyState",
  "unit": "export"
}
```

</details>
