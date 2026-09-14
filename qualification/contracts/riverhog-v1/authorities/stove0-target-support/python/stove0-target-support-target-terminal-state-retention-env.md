# stove0_target_support.TARGET_TERMINAL_STATE_RETENTION_ENV

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-target-terminal-sta-3f0337760a:2e3f43f6c0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c7212930da"></a>
- <a id="s-6d2cf9bf81"></a>`distribution`: `stove0-target-support`
- <a id="s-f255c6e048"></a>`module`: `stove0_target_support`
- <a id="s-fe4ac08936"></a>`name`: `TARGET_TERMINAL_STATE_RETENTION_ENV`
- <a id="s-508512ee99"></a>`unit`: `export`

### Declared structure

- <a id="s-4878d9a15f"></a>`kind`: `"constant"`
- <a id="s-238bfb195a"></a>`value`: `"STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS"`

## Governing policies

- <a id="pa-f4c60e0ac6"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TARGET_TERMINAL_STATE_RETENTION_ENV`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 64814778a17b7af6ae9b232581e3f69020981d456cfd12eb754dc83681118fd6 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS"
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TARGET_TERMINAL_STATE_RETENTION_ENV",
  "unit": "export"
}
```
