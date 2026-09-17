# stove0_core.Stove0WorkService.activate_preplanned

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0workservice-activate-preplanned:c84608ad1b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e39cfbca2d"></a>
- <a id="s-9201192902"></a>`distribution`: `stove0-server`
- <a id="s-f90b2b4805"></a>`module`: `stove0_core`
- <a id="s-26f34da71c"></a>`name`: `activate_preplanned`
- <a id="s-fb25705ade"></a>`owner`: `stove0_core.Stove0WorkService`
- <a id="s-c13bbf2e94"></a>`unit`: `member`

### Declared structure

- <a id="s-f2661f4194"></a>`kind`: `"method"`
- <a id="s-b25635d0ed"></a>`signature`: `"\"(self, work_id: 'str', *, expected_revision: 'int') -> 'WorkRecord'\""`

## Maintained corroboration

### Related interface records

- [Stove0WorkService](stove0-core-stove0workservice.md)

## Governing policies

- <a id="pa-00dbcf3c48"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.Stove0WorkService.activate_preplanned`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: effafc8f2d9b9fb9ccee43665ce8761d91295ed3e367b3e96b07e6c83c8e952e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', *, expected_revision: 'int') -> 'WorkRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "activate_preplanned",
  "owner": "stove0_core.Stove0WorkService",
  "unit": "member"
}
```

</details>
