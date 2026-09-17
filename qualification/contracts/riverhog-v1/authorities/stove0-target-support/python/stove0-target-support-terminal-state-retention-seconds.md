# stove0_target_support.terminal_state_retention_seconds

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-terminal-state-rete-e0fc154f23:59c6671b78 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f68066444a"></a>
- <a id="s-6b4186d8f3"></a>`distribution`: `stove0-target-support`
- <a id="s-6537236248"></a>`module`: `stove0_target_support`
- <a id="s-6076ecb003"></a>`name`: `terminal_state_retention_seconds`
- <a id="s-65f295c945"></a>`unit`: `export`

### Declared structure

- <a id="s-9917e892ae"></a>`kind`: `"function"`
- <a id="s-b2a7c4a35f"></a>`signature`: `"\"(environ: 'Mapping[str, str] \| None' = None) -> 'int'\""`

## Governing policies

- <a id="pa-da45e4a517"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — [reference/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.terminal_state_retention_seconds`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b4e439de929e671dcf6c69e93de5220c5282296e433e9ef8e34304391afb7a5c -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(environ: 'Mapping[str, str] | None' = None) -> 'int'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "terminal_state_retention_seconds",
  "unit": "export"
}
```

</details>
