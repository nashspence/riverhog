# stove0_core.AbandonOutcome

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-abandonoutcome:62f3d5b23b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f87d5cf27c"></a>
- <a id="s-853730936b"></a>`distribution`: `stove0-server`
- <a id="s-7468926c3b"></a>`module`: `stove0_core`
- <a id="s-58df0050a8"></a>`name`: `AbandonOutcome`
- <a id="s-8e7835fc4f"></a>`unit`: `export`

### Declared structure

- <a id="s-ec8dcb9094"></a>`kind`: `"object"`
- <a id="s-12d833eac8"></a>`type`: `"typing._LiteralGenericAlias"`

## Governing policies

- <a id="pa-9a2a3866d9"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.AbandonOutcome`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9fd865761d3d7ecc2c8f47c89b67e91c25ab795c7625451298d7573e204a3c98 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "typing._LiteralGenericAlias"
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "AbandonOutcome",
  "unit": "export"
}
```

</details>
