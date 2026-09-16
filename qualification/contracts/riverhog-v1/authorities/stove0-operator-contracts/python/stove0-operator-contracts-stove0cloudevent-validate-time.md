# stove0_operator_contracts.Stove0CloudEvent.validate_time

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-stove0cloudeven-6d07785561:7e94ebfcf3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a5b101eb0d"></a>
- <a id="s-165b818ea8"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-03b6abf95b"></a>`module`: `stove0_operator_contracts`
- <a id="s-8e24f962ce"></a>`name`: `validate_time`
- <a id="s-3325e5a252"></a>`owner`: `stove0_operator_contracts.Stove0CloudEvent`
- <a id="s-ba985660f3"></a>`unit`: `member`

### Declared structure

- <a id="s-7a196e8079"></a>`kind`: `"classmethod"`
- <a id="s-17b67d9361"></a>`signature`: `"\"(cls, value: 'str') -> 'str'\""`

## Maintained corroboration

### Related interface records

- [Stove0CloudEvent](stove0-operator-contracts-stove0cloudevent.md)

## Governing policies

- <a id="pa-3bbcda3e7d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.Stove0CloudEvent.validate_time`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 73f7ff7637ccc3a1e7ce2e7ad6f9348af9de3307300f291a5b40b5550662d3e8 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'str') -> 'str'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "validate_time",
  "owner": "stove0_operator_contracts.Stove0CloudEvent",
  "unit": "member"
}
```

</details>
