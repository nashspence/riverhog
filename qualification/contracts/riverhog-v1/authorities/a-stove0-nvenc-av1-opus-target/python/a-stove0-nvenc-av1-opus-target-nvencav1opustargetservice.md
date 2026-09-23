# a_stove0_nvenc_av1_opus_target.NvencAv1OpusTargetService

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-nvenc-av1-opus-target:a-stove0-nvenc-av1-opus-target-nvencav1op-97a9857cb8:a2f5e58357 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-nvenc-av1-opus-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-597a8b679e"></a>
- <a id="s-d36ba097b7"></a>`distribution`: `a-stove0-nvenc-av1-opus-target`
- <a id="s-33866ab724"></a>`module`: `a_stove0_nvenc_av1_opus_target`
- <a id="s-4b40541387"></a>`name`: `NvencAv1OpusTargetService`
- <a id="s-7d5a07fe4d"></a>`unit`: `export`

### Declared structure

- <a id="s-e6da6ef123"></a>`kind`: `"class"`
- <a id="s-5b2c723653"></a>`signature`: `"\"(*, state_root: 'Path', workspace_root: 'Path', ffmpeg: 'str' = 'ffmpeg', source_revision: 'str' = 'unknown', image_id: 'str', terminal_state_retention_seconds: 'int' = 2592000) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [preflight](a-stove0-nvenc-av1-opus-target-nvencav1opustargetservice-preflight.md)
- [descriptor](a-stove0-nvenc-av1-opus-target-nvencav1opustargetservice-descriptor.md)
- [prune_terminal_state](a-stove0-nvenc-av1-opus-target-nvencav1opustargetservice-prune-terminal-state.md)
- [get_job](a-stove0-nvenc-av1-opus-target-nvencav1opustargetservice-get-job.md)
- [put_job](a-stove0-nvenc-av1-opus-target-nvencav1opustargetservice-put-job.md)
- [cancel_job](a-stove0-nvenc-av1-opus-target-nvencav1opustargetservice-cancel-job.md)
- [close](a-stove0-nvenc-av1-opus-target-nvencav1opustargetservice-close.md)

## Governing policies

- <a id="pa-5f5f433a36"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-nvenc-av1-opus-target:a_stove0_nvenc_av1_opus_target](../../../evidence/sources/authorities.md#src-a28a72aec7) — [some-implementations/stove0/targets/nvenc-av1-opus/target/src/a\_stove0\_nvenc\_av1\_opus\_target/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/nvenc-av1-opus/target/src/a_stove0_nvenc_av1_opus_target/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_nvenc_av1_opus_target.NvencAv1OpusTargetService`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 418649c1286d2bb058d8380afe9b1b504279960f6ac5c5c63fd24cf26b15c55d -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(*, state_root: 'Path', workspace_root: 'Path', ffmpeg: 'str' = 'ffmpeg', source_revision: 'str' = 'unknown', image_id: 'str', terminal_state_retention_seconds: 'int' = 2592000) -> 'None'\""
  },
  "distribution": "a-stove0-nvenc-av1-opus-target",
  "module": "a_stove0_nvenc_av1_opus_target",
  "name": "NvencAv1OpusTargetService",
  "unit": "export"
}
```

</details>
