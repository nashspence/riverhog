# stove0_core.Stove0WorkService.admit_join

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0workservice-admit-join:f7015b53d0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ad1291f817"></a>
- <a id="s-cec2d153e9"></a>`distribution`: `stove0-server`
- <a id="s-4ec4e16394"></a>`module`: `stove0_core`
- <a id="s-edd591ca7b"></a>`name`: `admit_join`
- <a id="s-746529a08e"></a>`owner`: `stove0_core.Stove0WorkService`
- <a id="s-1146a71118"></a>`unit`: `member`

### Declared structure

- <a id="s-d35aeead63"></a>`kind`: `"method"`
- <a id="s-8811c78425"></a>`signature`: `"\"(self, work_id: 'str', plan: 'JoinPlan', selections: 'Sequence[ArtifactSelection]', *, expected_revision: 'int') -> 'WorkRecord'\""`

## Maintained corroboration

### Related interface records

- [Stove0WorkService](stove0-core-stove0workservice.md)

## Governing policies

- <a id="pa-e726afee11"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.Stove0WorkService.admit_join`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5587e56b16f6e58f8e4190f4bc9909822260e71dfc3ac8844a95ca6c11f2ca80 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', plan: 'JoinPlan', selections: 'Sequence[ArtifactSelection]', *, expected_revision: 'int') -> 'WorkRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "admit_join",
  "owner": "stove0_core.Stove0WorkService",
  "unit": "member"
}
```

</details>
