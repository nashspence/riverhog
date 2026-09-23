# stove0_protocol.Sha256

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-sha256:ca7bc29569 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3f16a86197"></a>
- <a id="s-6ab0df0d24"></a>`distribution`: `stove0-protocol`
- <a id="s-104ae8d0c4"></a>`module`: `stove0_protocol`
- <a id="s-8f9e8a159b"></a>`name`: `Sha256`
- <a id="s-1bb669113a"></a>`unit`: `export`

### Declared structure

- <a id="s-a74a8501b2"></a>`kind`: `"object"`
- <a id="s-38d1768681"></a>`type`: `"typing._AnnotatedAlias"`

## Governing policies

- <a id="pa-fde159286e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.Sha256`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6b048aa17d29295d42a68a11d891a84954bb6d485aa49363d53ee2ab3e81f5fd -->

```json
{
  "contract": {
    "kind": "object",
    "type": "typing._AnnotatedAlias"
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "Sha256",
  "unit": "export"
}
```

</details>
