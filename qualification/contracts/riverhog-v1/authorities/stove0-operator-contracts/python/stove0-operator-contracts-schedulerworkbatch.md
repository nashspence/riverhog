# stove0_operator_contracts.SchedulerWorkBatch

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-schedulerworkbatch:afc2db9aed -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d876608274"></a>
| Field | Shape |
|---|---|
| <a id="s-92d7873cc7"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-9870bed9cd"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-9628750893"></a>`module` | "stove0_operator_contracts" |
| <a id="s-f7c8a8072b"></a>`name` | "SchedulerWorkBatch" |
| <a id="s-3b3255e2fc"></a>`unit` | "export" |

## Governing policies

- <a id="pa-fe676e9736"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.SchedulerWorkBatch`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: be51770b8585c1e34f68457dde2ed9a32100000e94480de5fd9f4be975459db4 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "84e8e1e2198f4b04c8d33af067097705e328f79c575320420d0034fca2160a60",
    "signature": "\"(*, role: Literal['controller', 'worker', 'combined'], cursor: str, next_cursor: str, progressed: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], ...], failures: tuple[stove0_operator_contracts.SchedulerFailure, ...]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "SchedulerWorkBatch",
  "unit": "export"
}
```
