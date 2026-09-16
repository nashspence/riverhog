# stove0_target_support.TargetService.contract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetservice-contract:b3ccc86322 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3277a1588c"></a>
- <a id="s-25eafec5a9"></a>`distribution`: `stove0-target-support`
- <a id="s-7336dd61f6"></a>`module`: `stove0_target_support`
- <a id="s-0e8f296c84"></a>`name`: `contract`
- <a id="s-b146d8763f"></a>`owner`: `stove0_target_support.TargetService`
- <a id="s-ba2dbaf6ef"></a>`unit`: `member`

### Declared structure

- <a id="s-4c5d3a262a"></a>`kind`: `"method"`
- <a id="s-cf11212303"></a>`signature`: `"\"(self) -> 'TargetContract'\""`

## Maintained corroboration

### Related interface records

- [TargetService](stove0-target-support-targetservice.md)

## Governing policies

- <a id="pa-2e9b497f46"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetService.contract`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5caf3f87eff2736ef5b8b8c3f55073bd7b9f8c8f2b866767ff6129cb1863df70 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'TargetContract'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "contract",
  "owner": "stove0_target_support.TargetService",
  "unit": "member"
}
```

</details>
