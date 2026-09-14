# stove0_target_support.TRANSFORM_TARGET_PROTOCOL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-transform-target-protocol:19266b546f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3c72ec56e4"></a>
- <a id="s-cfbf389044"></a>`distribution`: `stove0-target-support`
- <a id="s-475d550d24"></a>`module`: `stove0_target_support`
- <a id="s-d01cc5cdbb"></a>`name`: `TRANSFORM_TARGET_PROTOCOL`
- <a id="s-cdae3c29c7"></a>`unit`: `export`

### Declared structure

- <a id="s-70e5f3d467"></a>`kind`: `"constant"`
- <a id="s-1127e1f271"></a>`value`: `"stove0-transform-target/v1"`

## Governing policies

- <a id="pa-6a51d0b918"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TRANSFORM_TARGET_PROTOCOL`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1c6d5591769d1ab3b6796e9c2d94262fa8b7821e3931a032e90d7c0c06501131 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0-transform-target/v1"
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TRANSFORM_TARGET_PROTOCOL",
  "unit": "export"
}
```
