# stove0_protocol.SourceCollectionRetirementPolicy

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-sourcecollectionretirementpolicy:43d744c804 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1a30263b89"></a>
- <a id="s-530a85bcbc"></a>`distribution`: `stove0-protocol`
- <a id="s-6ad89c3e31"></a>`module`: `stove0_protocol`
- <a id="s-d34017c835"></a>`name`: `SourceCollectionRetirementPolicy`
- <a id="s-6c6356ad1a"></a>`unit`: `export`

### Declared structure

- <a id="s-736f155b68"></a>`kind`: `"object"`
- <a id="s-024f349d80"></a>`type`: `"typing._LiteralGenericAlias"`

## Governing policies

- <a id="pa-cc01517fb8"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.SourceCollectionRetirementPolicy`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7ec10bb021eaced4fef24841fa79795aeddb8c7bc06d7b378fa5cb82e9ab3328 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "typing._LiteralGenericAlias"
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "SourceCollectionRetirementPolicy",
  "unit": "export"
}
```

</details>
