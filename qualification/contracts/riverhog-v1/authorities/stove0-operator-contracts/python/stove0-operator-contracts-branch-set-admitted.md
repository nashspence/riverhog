# stove0_operator_contracts.BRANCH_SET_ADMITTED

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-branch-set-admitted:10fe6c1184 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ca3be777b2"></a>
- <a id="s-e11a314824"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-9e6bb8a0ae"></a>`module`: `stove0_operator_contracts`
- <a id="s-d693daaa52"></a>`name`: `BRANCH_SET_ADMITTED`
- <a id="s-2567a8406b"></a>`unit`: `export`

### Declared structure

- <a id="s-fa1f9cac90"></a>`kind`: `"constant"`
- <a id="s-c2a02c9f08"></a>`value`: `"io.riverhog.stove0.branch-set.admitted"`

## Governing policies

- <a id="pa-5134bf02d3"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [reference/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.BRANCH_SET_ADMITTED`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3b3a8a324ed6a14a4bbb1fec6781bca1a63d17ec7ee857291dfefb8176204404 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "io.riverhog.stove0.branch-set.admitted"
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "BRANCH_SET_ADMITTED",
  "unit": "export"
}
```

</details>
