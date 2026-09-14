# stove0_target_support.InputArtifactContract.validate_cardinality

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-inputartifactcontra-1736f553ed:388c00419c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e232524161"></a>
- <a id="s-4f6f86b2e6"></a>`distribution`: `stove0-target-support`
- <a id="s-3832ff2ac5"></a>`module`: `stove0_target_support`
- <a id="s-590c2602f7"></a>`name`: `validate_cardinality`
- <a id="s-31a6d04c39"></a>`owner`: `stove0_target_support.InputArtifactContract`
- <a id="s-2e5c62c87e"></a>`unit`: `member`

### Declared structure

- <a id="s-e5ecc7d101"></a>`kind`: `"method"`
- <a id="s-830f5ad6eb"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [stove0_target_support.InputArtifactContract](stove0-target-support-inputartifactcontract.md)

## Governing policies

- <a id="pa-16129d56d5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.InputArtifactContract.validate_cardinality`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d59a190f41d53bf93788fdf0a72505fbb517edc6283753f8302882627fcb1d65 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "validate_cardinality",
  "owner": "stove0_target_support.InputArtifactContract",
  "unit": "member"
}
```
