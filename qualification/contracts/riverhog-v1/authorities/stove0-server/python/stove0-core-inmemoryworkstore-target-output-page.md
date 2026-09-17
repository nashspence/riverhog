# stove0_core.InMemoryWorkStore.target_output_page

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-inmemoryworkstore-target-output-page:b849d349be -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8fbffe3314"></a>
- <a id="s-26c500236f"></a>`distribution`: `stove0-server`
- <a id="s-4ce312496b"></a>`module`: `stove0_core`
- <a id="s-2ebdae5ef2"></a>`name`: `target_output_page`
- <a id="s-a6c8b840a6"></a>`owner`: `stove0_core.InMemoryWorkStore`
- <a id="s-dd3c6117ba"></a>`unit`: `member`

### Declared structure

- <a id="s-ed68cd2354"></a>`kind`: `"method"`
- <a id="s-7234e26155"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str', *, after_id: 'str \| None', limit: 'int') -> 'tuple[OutputArtifact, ...]'\""`

## Maintained corroboration

### Related interface records

- [InMemoryWorkStore](stove0-core-inmemoryworkstore.md)

## Governing policies

- <a id="pa-7ebcd80854"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.InMemoryWorkStore.target_output_page`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8591b2eda4e2c917231f4419f67652902b4f9230d5806d1b469b8d52d26d5bf1 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str', *, after_id: 'str | None', limit: 'int') -> 'tuple[OutputArtifact, ...]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "target_output_page",
  "owner": "stove0_core.InMemoryWorkStore",
  "unit": "member"
}
```

</details>
