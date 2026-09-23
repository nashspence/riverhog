# stove0_core.Stove0WorkService.record_observation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0workservice-record-observation:04e5686048 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-dbe30c8956"></a>
- <a id="s-3420ccfe3b"></a>`distribution`: `stove0-server`
- <a id="s-e760a78374"></a>`module`: `stove0_core`
- <a id="s-cbc9a10774"></a>`name`: `record_observation`
- <a id="s-2d7555fe4b"></a>`owner`: `stove0_core.Stove0WorkService`
- <a id="s-abf295f15f"></a>`unit`: `member`

### Declared structure

- <a id="s-53f3c2e3e2"></a>`kind`: `"method"`
- <a id="s-032d904ec7"></a>`signature`: `"\"(self, work_id: 'str', result: 'ContentObservationResult', *, expected_revision: 'int') -> 'WorkRecord'\""`

## Maintained corroboration

### Related interface records

- [Stove0WorkService](stove0-core-stove0workservice.md)

## Governing policies

- <a id="pa-c1261d3103"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.Stove0WorkService.record_observation`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f99b0bd07a29125b294c86e7e732baeba877c0531e46e6de45f20701294ca313 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', result: 'ContentObservationResult', *, expected_revision: 'int') -> 'WorkRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "record_observation",
  "owner": "stove0_core.Stove0WorkService",
  "unit": "member"
}
```

</details>
