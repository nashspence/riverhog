# a_stove0_opus_target.OpusTargetService

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-opus-target:a-stove0-opus-target-opustargetservice:1168aef8d4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-opus-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1e60f93343"></a>
- <a id="s-b6b0d5cf10"></a>`distribution`: `a-stove0-opus-target`
- <a id="s-69405d1054"></a>`module`: `a_stove0_opus_target`
- <a id="s-f6edc872b7"></a>`name`: `OpusTargetService`
- <a id="s-bedce955cd"></a>`unit`: `export`

### Declared structure

- <a id="s-40cc4c3a67"></a>`kind`: `"class"`
- <a id="s-d9d059c90c"></a>`signature`: `"\"(*, state_root: 'Path', workspace_root: 'Path', ffmpeg: 'str' = 'ffmpeg', source_revision: 'str' = 'unknown', image_digest: 'str', terminal_state_retention_seconds: 'int' = 2592000) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [cancel_job](a-stove0-opus-target-opustargetservice-cancel-job.md)
- [close](a-stove0-opus-target-opustargetservice-close.md)
- [descriptor](a-stove0-opus-target-opustargetservice-descriptor.md)
- [get_job](a-stove0-opus-target-opustargetservice-get-job.md)
- [prune_terminal_state](a-stove0-opus-target-opustargetservice-prune-terminal-state.md)
- [preflight](a-stove0-opus-target-opustargetservice-preflight.md)
- [put_job](a-stove0-opus-target-opustargetservice-put-job.md)

## Governing policies

- <a id="pa-48a747dbf3"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-opus-target:a_stove0_opus_target](../../../evidence/sources/authorities.md#src-43f70c74af) — [some-implementations/stove0/targets/opus/target/src/a\_stove0\_opus\_target/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/opus/target/src/a_stove0_opus_target/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_opus_target.OpusTargetService`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f68d941048d2dc46ff4a6486bc8912adaaeb3a350d43134dbb3ef7c2be9d2820 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(*, state_root: 'Path', workspace_root: 'Path', ffmpeg: 'str' = 'ffmpeg', source_revision: 'str' = 'unknown', image_digest: 'str', terminal_state_retention_seconds: 'int' = 2592000) -> 'None'\""
  },
  "distribution": "a-stove0-opus-target",
  "module": "a_stove0_opus_target",
  "name": "OpusTargetService",
  "unit": "export"
}
```

</details>
