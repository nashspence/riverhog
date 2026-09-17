# stove0_core.Stove0WorkService.begin_observations

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0workservice-begin-observations:30c3ecce53 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c616522c73"></a>
- <a id="s-f598495c4a"></a>`distribution`: `stove0-server`
- <a id="s-788d9f71bf"></a>`module`: `stove0_core`
- <a id="s-bd1539e6a1"></a>`name`: `begin_observations`
- <a id="s-412f200dc4"></a>`owner`: `stove0_core.Stove0WorkService`
- <a id="s-e4edb6ed53"></a>`unit`: `member`

### Declared structure

- <a id="s-480fd9fd4c"></a>`kind`: `"method"`
- <a id="s-79f46a8f98"></a>`signature`: `"\"(self, work_id: 'str', requests: 'Sequence[ObservationRequest]', *, expected_revision: 'int') -> 'WorkRecord'\""`

## Maintained corroboration

### Related interface records

- [Stove0WorkService](stove0-core-stove0workservice.md)

## Governing policies

- <a id="pa-14c9cec4d0"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.Stove0WorkService.begin_observations`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7444ad71a062e65ebb70f3768b013f55d26b20cd10e5105035c20428d94c3b21 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', requests: 'Sequence[ObservationRequest]', *, expected_revision: 'int') -> 'WorkRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "begin_observations",
  "owner": "stove0_core.Stove0WorkService",
  "unit": "member"
}
```

</details>
