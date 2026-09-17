# stove0_core.SchedulerRole

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-schedulerrole:44d1bd4ec8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e2c6231ab0"></a>
- <a id="s-8fbeb6c32d"></a>`distribution`: `stove0-server`
- <a id="s-37e618a9fd"></a>`module`: `stove0_core`
- <a id="s-257c17db9b"></a>`name`: `SchedulerRole`
- <a id="s-269ad3c6ba"></a>`unit`: `export`

### Declared structure

- <a id="s-7405327c6b"></a>`kind`: `"object"`
- <a id="s-f02edbd65d"></a>`type`: `"typing._LiteralGenericAlias"`

## Governing policies

- <a id="pa-011d24ee19"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.SchedulerRole`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1d9bfc4580c2ffefc7766cc38d3babf600dcb40cfbd66fe00112381ff10d784b -->

```json
{
  "contract": {
    "kind": "object",
    "type": "typing._LiteralGenericAlias"
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "SchedulerRole",
  "unit": "export"
}
```

</details>
