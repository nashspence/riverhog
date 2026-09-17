# stove0_core.WorkStore.target_output_page

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-workstore-target-output-page:bd1ba2ab29 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3da9a10448"></a>
- <a id="s-eead5c0eaf"></a>`distribution`: `stove0-server`
- <a id="s-cc461f6af6"></a>`module`: `stove0_core`
- <a id="s-4ffc3ac05d"></a>`name`: `target_output_page`
- <a id="s-1cbb405091"></a>`owner`: `stove0_core.WorkStore`
- <a id="s-145040a5a9"></a>`unit`: `member`

### Declared structure

- <a id="s-4d76e7d1a4"></a>`kind`: `"method"`
- <a id="s-9a754b8aed"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str', *, after_id: 'str \| None', limit: 'int') -> 'tuple[OutputArtifact, ...]'\""`

## Maintained corroboration

### Related interface records

- [WorkStore](stove0-core-workstore.md)

## Governing policies

- <a id="pa-c9b7ae3f99"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.WorkStore.target_output_page`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e02cd6c40deb4d20ba98f69cfccc091844b504ef54c8fc9bc2b5baba5cc0ec4f -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str', *, after_id: 'str | None', limit: 'int') -> 'tuple[OutputArtifact, ...]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "target_output_page",
  "owner": "stove0_core.WorkStore",
  "unit": "member"
}
```

</details>
