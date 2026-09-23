# stove0_core.Stove0WorkService.rebind_claim

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0workservice-rebind-claim:3393f75fdd -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-434cff48da"></a>
- <a id="s-ac16c9f6eb"></a>`distribution`: `stove0-server`
- <a id="s-9ac7b319f4"></a>`module`: `stove0_core`
- <a id="s-9032112ce5"></a>`name`: `rebind_claim`
- <a id="s-5e1257757d"></a>`owner`: `stove0_core.Stove0WorkService`
- <a id="s-bc6b3ee9ce"></a>`unit`: `member`

### Declared structure

- <a id="s-28068c6916"></a>`kind`: `"method"`
- <a id="s-57da5c5083"></a>`signature`: `"\"(self, work_id: 'str', *, claim_id: 'str', fence: 'int', expected_revision: 'int') -> 'WorkRecord'\""`

## Maintained corroboration

### Related interface records

- [Stove0WorkService](stove0-core-stove0workservice.md)

## Governing policies

- <a id="pa-b4d603521c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.Stove0WorkService.rebind_claim`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 94b3542ba47a1862d294cbb493929cb75e44433c68a56bf2e3c8eed8f396a1a0 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', *, claim_id: 'str', fence: 'int', expected_revision: 'int') -> 'WorkRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "rebind_claim",
  "owner": "stove0_core.Stove0WorkService",
  "unit": "member"
}
```

</details>
