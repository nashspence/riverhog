# a_review0_materializer.ReviewMaterializeTargetService

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-review0-materializer:a-review0-materializer-reviewmaterializet-9098abb4bc:66057d8a79 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-materializer](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1e0a990955"></a>
- <a id="s-8901f5cf6f"></a>`distribution`: `a-review0-materializer`
- <a id="s-2e8cbfcacc"></a>`module`: `a_review0_materializer`
- <a id="s-c596d221d5"></a>`name`: `ReviewMaterializeTargetService`
- <a id="s-356d5117ee"></a>`unit`: `export`

### Declared structure

- <a id="s-56d8f89e50"></a>`kind`: `"class"`
- <a id="s-dfceb3d6de"></a>`signature`: `"\"(*, state_root: 'Path', workspace_root: 'Path', samplers: 'tuple[SamplerRegistration, ...]', source_revision: 'str' = 'unknown', image_digest: 'str', implementation_version: 'str', terminal_state_retention_seconds: 'int' = 2592000) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [descriptor](a-review0-materializer-reviewmaterializetargetservice-descriptor.md)
- [readiness](a-review0-materializer-reviewmaterializetargetservice-readiness.md)
- [prune_terminal_state](a-review0-materializer-reviewmaterializetargetservice-prune-terminal-state.md)
- [preflight](a-review0-materializer-reviewmaterializetargetservice-preflight.md)
- [cancel_job](a-review0-materializer-reviewmaterializetargetservice-cancel-job.md)
- [get_job](a-review0-materializer-reviewmaterializetargetservice-get-job.md)
- [put_job](a-review0-materializer-reviewmaterializetargetservice-put-job.md)
- [close](a-review0-materializer-reviewmaterializetargetservice-close.md)

## Governing policies

- <a id="pa-bd935f03ee"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-review0-materializer:a_review0_materializer](../../../evidence/sources/authorities.md#src-fa69f32f84) — [some-implementations/stove0/review0/materialize-target/src/a\_review0\_materializer/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/materialize-target/src/a_review0_materializer/__init__.py)

### Machine authority

- `/external_contract/python/a_review0_materializer.ReviewMaterializeTargetService`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c04388a75acce67524b97a1fa70dfd72d11598c5cc4df9d299edb58637cc1e76 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(*, state_root: 'Path', workspace_root: 'Path', samplers: 'tuple[SamplerRegistration, ...]', source_revision: 'str' = 'unknown', image_digest: 'str', implementation_version: 'str', terminal_state_retention_seconds: 'int' = 2592000) -> 'None'\""
  },
  "distribution": "a-review0-materializer",
  "module": "a_review0_materializer",
  "name": "ReviewMaterializeTargetService",
  "unit": "export"
}
```

</details>
