# stove0_protocol.BranchPlan.bind_child_work

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-branchplan-bind-child-work:2e3debab19 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-dfc9db5e6e"></a>
- <a id="s-b02f3b4146"></a>`distribution`: `stove0-protocol`
- <a id="s-63f4c714c2"></a>`module`: `stove0_protocol`
- <a id="s-8a2223eeb3"></a>`name`: `bind_child_work`
- <a id="s-482129dee6"></a>`owner`: `stove0_protocol.BranchPlan`
- <a id="s-4cbf3f7ec3"></a>`unit`: `member`

### Declared structure

- <a id="s-0ab44570f2"></a>`kind`: `"method"`
- <a id="s-a4109e7234"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [BranchPlan](stove0-protocol-branchplan.md)

## Governing policies

- <a id="pa-62cfd3a529"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.BranchPlan.bind_child_work`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3be89c4df438628398edd2a228d9d6f8005ddd6c2babfdd581021611939fb616 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "bind_child_work",
  "owner": "stove0_protocol.BranchPlan",
  "unit": "member"
}
```

</details>
