# stove0_core.Stove0WorkService.fail

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0workservice-fail:64ac0ad315 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-dc0cf20d29"></a>
- <a id="s-49aefb2a4e"></a>`distribution`: `stove0-server`
- <a id="s-99805f374a"></a>`module`: `stove0_core`
- <a id="s-9a360c17e8"></a>`name`: `fail`
- <a id="s-b005fcbaec"></a>`owner`: `stove0_core.Stove0WorkService`
- <a id="s-6251aa8d89"></a>`unit`: `member`

### Declared structure

- <a id="s-ee5b6be6a0"></a>`kind`: `"method"`
- <a id="s-4d342c7a91"></a>`signature`: `"\"(self, work_id: 'str', failure: 'WorkFailure', *, expected_revision: 'int') -> 'WorkRecord'\""`

## Maintained corroboration

### Related interface records

- [Stove0WorkService](stove0-core-stove0workservice.md)

## Governing policies

- <a id="pa-617a87c572"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.Stove0WorkService.fail`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d3eece7c87c5d6df92effdf826099ee61e6fe75c133ab5f9ecbb480a681a002a -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', failure: 'WorkFailure', *, expected_revision: 'int') -> 'WorkRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "fail",
  "owner": "stove0_core.Stove0WorkService",
  "unit": "member"
}
```

</details>
