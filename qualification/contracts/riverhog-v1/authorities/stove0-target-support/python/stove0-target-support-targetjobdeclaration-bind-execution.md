# stove0_target_support.TargetJobDeclaration.bind_execution

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetjobdeclaratio-1f02874078:9d3f5b9fb4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-dba944a1c8"></a>
- <a id="s-824e25db2f"></a>`distribution`: `stove0-target-support`
- <a id="s-640022374e"></a>`module`: `stove0_target_support`
- <a id="s-7796d79b38"></a>`name`: `bind_execution`
- <a id="s-0170a79f1d"></a>`owner`: `stove0_target_support.TargetJobDeclaration`
- <a id="s-fed2efa300"></a>`unit`: `member`

### Declared structure

- <a id="s-643c0dc4db"></a>`kind`: `"method"`
- <a id="s-0d3dda83ee"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [stove0_target_support.TargetJobDeclaration](stove0-target-support-targetjobdeclaration.md)

## Governing policies

- <a id="pa-633627f988"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetJobDeclaration.bind_execution`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fd4a7f9b4c808071cd8758fe0c0da9cd96ed54a5b4ac80de2d523148c20b6dfc -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "bind_execution",
  "owner": "stove0_target_support.TargetJobDeclaration",
  "unit": "member"
}
```
