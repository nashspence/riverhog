# stove0_observer_support.observer_schema_bundle

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-observer-schema-bundle:5df1b49fd1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9ec37566d0"></a>
- <a id="s-1b7a06f132"></a>`distribution`: `stove0-observer-support`
- <a id="s-bb73cd7ff3"></a>`module`: `stove0_observer_support`
- <a id="s-b7a912118f"></a>`name`: `observer_schema_bundle`
- <a id="s-b25d014cd5"></a>`unit`: `export`

### Declared structure

- <a id="s-191dca9439"></a>`kind`: `"function"`
- <a id="s-2a4ff1b125"></a>`signature`: `"\"() -> 'dict[str, Any]'\""`

## Governing policies

- <a id="pa-9a66cbc21d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources.md#src-13bf3acd32) — `reference/stove0/packages/observer-support/src/stove0_observer_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_support.observer_schema_bundle`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fc71c5e0c0625c852454c5dcc660c03d1d1c43751a050a1fec8d8319ac53b296 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"() -> 'dict[str, Any]'\""
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "observer_schema_bundle",
  "unit": "export"
}
```

</details>
