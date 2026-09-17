# stove0_protocol.validate_branch_set_plan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-validate-branch-set-plan:b0db14e95c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5f14a5f313"></a>
- <a id="s-b3e42ff843"></a>`distribution`: `stove0-protocol`
- <a id="s-bee774afa5"></a>`module`: `stove0_protocol`
- <a id="s-e8b05b1484"></a>`name`: `validate_branch_set_plan`
- <a id="s-65f4917843"></a>`unit`: `export`

### Declared structure

- <a id="s-48de5f9de8"></a>`kind`: `"function"`
- <a id="s-1eb9ec92e6"></a>`signature`: `"\"(plan: 'BranchSetPlan', selections: 'SelectionDocuments', branch_sets: 'Mapping[str, BranchSetPlan] \| None' = None) -> 'None'\""`

## Governing policies

- <a id="pa-ab7c337cdf"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.validate_branch_set_plan`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ece919fe52867decea551f6ca4cd8d51b743da642e62adea9c211b785fa6f6a2 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(plan: 'BranchSetPlan', selections: 'SelectionDocuments', branch_sets: 'Mapping[str, BranchSetPlan] | None' = None) -> 'None'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "validate_branch_set_plan",
  "unit": "export"
}
```

</details>
