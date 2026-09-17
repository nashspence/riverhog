# stove0_core.WorkStore.target_disposition_page

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-workstore-target-disposition-page:e5dddc7202 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8f78df6056"></a>
- <a id="s-3ef5124ae7"></a>`distribution`: `stove0-server`
- <a id="s-204d0efbd8"></a>`module`: `stove0_core`
- <a id="s-d600a07cb7"></a>`name`: `target_disposition_page`
- <a id="s-ec54674676"></a>`owner`: `stove0_core.WorkStore`
- <a id="s-d1d94e9937"></a>`unit`: `member`

### Declared structure

- <a id="s-bb006f82f7"></a>`kind`: `"method"`
- <a id="s-7c26c25202"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str', *, after_id: 'str \| None', limit: 'int') -> 'tuple[InputDispositionDeclaration, ...]'\""`

## Maintained corroboration

### Related interface records

- [WorkStore](stove0-core-workstore.md)

## Governing policies

- <a id="pa-a6cb8501da"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.WorkStore.target_disposition_page`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ac977ba6a8a01bbe6ca23d610f7723f6fe23820809768c8c0df78f163461aef2 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str', *, after_id: 'str | None', limit: 'int') -> 'tuple[InputDispositionDeclaration, ...]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "target_disposition_page",
  "owner": "stove0_core.WorkStore",
  "unit": "member"
}
```

</details>
