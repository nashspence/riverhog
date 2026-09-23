# stove0_core.Stove0WorkService.activate_preplanned_coordination

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0workservice-activate-pr-1fa7611b47:183df06155 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e292ee342b"></a>
- <a id="s-a9ce76d4ad"></a>`distribution`: `stove0-server`
- <a id="s-4360d4a354"></a>`module`: `stove0_core`
- <a id="s-af197afafa"></a>`name`: `activate_preplanned_coordination`
- <a id="s-056ebabae8"></a>`owner`: `stove0_core.Stove0WorkService`
- <a id="s-d5b951368a"></a>`unit`: `member`

### Declared structure

- <a id="s-d732f45673"></a>`kind`: `"method"`
- <a id="s-1a8bbda57b"></a>`signature`: `"\"(self, work_id: 'str', *, expected_revision: 'int') -> 'WorkRecord'\""`

## Maintained corroboration

### Related interface records

- [Stove0WorkService](stove0-core-stove0workservice.md)

## Governing policies

- <a id="pa-4364e9a68b"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.Stove0WorkService.activate_preplanned_coordination`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 644cbd90600be0ef5c88aad90235c61833a8d0b4fcb6c2d356829b66e6bfe23c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', *, expected_revision: 'int') -> 'WorkRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "activate_preplanned_coordination",
  "owner": "stove0_core.Stove0WorkService",
  "unit": "member"
}
```

</details>
