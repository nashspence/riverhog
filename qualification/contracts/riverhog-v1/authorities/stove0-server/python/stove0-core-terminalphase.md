# stove0_core.TerminalPhase

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-terminalphase:1816595e51 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4add87bfad"></a>
- <a id="s-3608756bdb"></a>`distribution`: `stove0-server`
- <a id="s-cb16092efa"></a>`module`: `stove0_core`
- <a id="s-572da82620"></a>`name`: `TerminalPhase`
- <a id="s-8676f5ae22"></a>`unit`: `export`

### Declared structure

- <a id="s-0558081942"></a>`kind`: `"object"`
- <a id="s-258216e169"></a>`type`: `"typing._LiteralGenericAlias"`

## Governing policies

- <a id="pa-3832aacaad"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.TerminalPhase`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5dbd6b1e10587174afde4f0a9b034d95d934a8137bec9cee6d8d4340131e50c8 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "typing._LiteralGenericAlias"
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "TerminalPhase",
  "unit": "export"
}
```

</details>
