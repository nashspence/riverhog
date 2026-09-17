# stove0_opus_target.OpusTargetService

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-opus-target:stove0-opus-target-opustargetservice:73e0383b03 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-opus-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6b69dbaee7"></a>
- <a id="s-ea48b69c3f"></a>`distribution`: `stove0-opus-target`
- <a id="s-718414f4aa"></a>`module`: `stove0_opus_target`
- <a id="s-837dee7030"></a>`name`: `OpusTargetService`
- <a id="s-a7fcef65cb"></a>`unit`: `export`

### Declared structure

- <a id="s-65e5f9af1d"></a>`kind`: `"class"`
- <a id="s-38862c30fc"></a>`signature`: `"\"(*, state_root: 'Path', workspace_root: 'Path', ffmpeg: 'str' = 'ffmpeg', source_revision: 'str' = 'unknown', image_digest: 'str', terminal_state_retention_seconds: 'int' = 2592000) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [cancel_job](stove0-opus-target-opustargetservice-cancel-job.md)
- [close](stove0-opus-target-opustargetservice-close.md)
- [contract](stove0-opus-target-opustargetservice-contract.md)
- [get_job](stove0-opus-target-opustargetservice-get-job.md)
- [preflight](stove0-opus-target-opustargetservice-preflight.md)
- [prune_terminal_state](stove0-opus-target-opustargetservice-prune-terminal-state.md)
- [put_job](stove0-opus-target-opustargetservice-put-job.md)

## Governing policies

- <a id="pa-172e6114f5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-opus-target:stove0_opus_target](../../../evidence/sources.md#src-9164f15983) — [reference/stove0/targets/opus/target/src/stove0\_opus\_target/\_\_init\_\_.py](../../../../../../reference/stove0/targets/opus/target/src/stove0_opus_target/__init__.py)

### Machine authority

- `/external_contract/python/stove0_opus_target.OpusTargetService`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2c902add701b48207e35388def5f1ab6e2576674efca907cf87c6ed0191e7aaf -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(*, state_root: 'Path', workspace_root: 'Path', ffmpeg: 'str' = 'ffmpeg', source_revision: 'str' = 'unknown', image_digest: 'str', terminal_state_retention_seconds: 'int' = 2592000) -> 'None'\""
  },
  "distribution": "stove0-opus-target",
  "module": "stove0_opus_target",
  "name": "OpusTargetService",
  "unit": "export"
}
```

</details>
