# stove0_operator_contracts.DeparturePolicy.policy_sha256

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-departurepolicy-a6bdb02b92:1a5bc9fa68 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-55b6c592c0"></a>
- <a id="s-6fe4ad786a"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-b482634ac0"></a>`module`: `stove0_operator_contracts`
- <a id="s-93d810e385"></a>`name`: `policy_sha256`
- <a id="s-6d67e210e7"></a>`owner`: `stove0_operator_contracts.DeparturePolicy`
- <a id="s-f110c6c2ab"></a>`unit`: `member`

### Declared structure

- <a id="s-2fcc0496d0"></a>`kind`: `"property"`
- <a id="s-dc21f5e3f0"></a>`signature`: `"\"(self) -> 'str'\""`

## Maintained corroboration

### Related interface records

- [DeparturePolicy](stove0-operator-contracts-departurepolicy.md)

## Governing policies

- <a id="pa-5d90ce71f9"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.DeparturePolicy.policy_sha256`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a3f773b68e90b8a1910842d156165e5859a2666de9aa94e931e1712dfbef39ff -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> 'str'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "policy_sha256",
  "owner": "stove0_operator_contracts.DeparturePolicy",
  "unit": "member"
}
```

</details>
