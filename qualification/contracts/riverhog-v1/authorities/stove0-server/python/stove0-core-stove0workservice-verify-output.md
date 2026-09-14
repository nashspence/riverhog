# stove0_core.Stove0WorkService.verify_output

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0workservice-verify-output:2ace7b1c7d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2385d98653"></a>
- <a id="s-3108bd70c2"></a>`distribution`: `stove0-server`
- <a id="s-1129415ea9"></a>`module`: `stove0_core`
- <a id="s-4d1140dae4"></a>`name`: `verify_output`
- <a id="s-9975065c1d"></a>`owner`: `stove0_core.Stove0WorkService`
- <a id="s-993c71da33"></a>`unit`: `member`

### Declared structure

- <a id="s-b3c1aafcea"></a>`kind`: `"method"`
- <a id="s-e3528d3e76"></a>`signature`: `"\"(self, work_id: 'str', output: 'OutputCollectionRef', settlement: 'TargetSettlementAuthority', *, expected_revision: 'int') -> 'WorkRecord'\""`

## Maintained corroboration

### Related interface records

- [stove0_core.Stove0WorkService](stove0-core-stove0workservice.md)

## Governing policies

- <a id="pa-b564cd80a7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.Stove0WorkService.verify_output`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3c88c436e088bb8b336aab3eeaea87558dd2706548c3a8fdfc990b501473a354 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', output: 'OutputCollectionRef', settlement: 'TargetSettlementAuthority', *, expected_revision: 'int') -> 'WorkRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "verify_output",
  "owner": "stove0_core.Stove0WorkService",
  "unit": "member"
}
```
