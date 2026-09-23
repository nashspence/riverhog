# a_review0_rclone_target.ReviewRcloneEffectTargetService

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-review0-rclone-target:a-review0-rclone-target-reviewrcloneeffec-b0d1389871:1aa459a479 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-rclone-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6cd34ea087"></a>
- <a id="s-15d171d222"></a>`distribution`: `a-review0-rclone-target`
- <a id="s-2c3ba4d66c"></a>`module`: `a_review0_rclone_target`
- <a id="s-4b80825198"></a>`name`: `ReviewRcloneEffectTargetService`
- <a id="s-214bc43a04"></a>`unit`: `export`

### Declared structure

- <a id="s-8b9ac96b70"></a>`kind`: `"class"`
- <a id="s-f2edb9d68b"></a>`signature`: `"\"(*, state_root: 'Path', workspace_root: 'Path', samplers: 'tuple[SamplerRegistration, ...]', destination: 'RcloneReviewDestination', source_revision: 'str' = 'unknown', image_digest: 'str', implementation_version: 'str', terminal_state_retention_seconds: 'int' = 2592000) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [preflight](a-review0-rclone-target-reviewrcloneeffecttargetservice-preflight.md)
- [put_job](a-review0-rclone-target-reviewrcloneeffecttargetservice-put-job.md)
- [cancel_job](a-review0-rclone-target-reviewrcloneeffecttargetservice-cancel-job.md)
- [prune_terminal_state](a-review0-rclone-target-reviewrcloneeffecttargetservice-prune-terminal-state.md)
- [readiness](a-review0-rclone-target-reviewrcloneeffecttargetservice-readiness.md)
- [descriptor](a-review0-rclone-target-reviewrcloneeffecttargetservice-descriptor.md)
- [get_job](a-review0-rclone-target-reviewrcloneeffecttargetservice-get-job.md)
- [close](a-review0-rclone-target-reviewrcloneeffecttargetservice-close.md)

## Governing policies

- <a id="pa-ffddee2d6c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-review0-rclone-target:a_review0_rclone_target](../../../evidence/sources/authorities.md#src-b6b8161d65) — [some-implementations/stove0/review0/rclone-effect-target/src/a\_review0\_rclone\_target/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/rclone-effect-target/src/a_review0_rclone_target/__init__.py)

### Machine authority

- `/external_contract/python/a_review0_rclone_target.ReviewRcloneEffectTargetService`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a92c4ceafae18f03f7db97229912bebbb1293bbe55fdb901dc0ac6e7eaf017e3 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(*, state_root: 'Path', workspace_root: 'Path', samplers: 'tuple[SamplerRegistration, ...]', destination: 'RcloneReviewDestination', source_revision: 'str' = 'unknown', image_digest: 'str', implementation_version: 'str', terminal_state_retention_seconds: 'int' = 2592000) -> 'None'\""
  },
  "distribution": "a-review0-rclone-target",
  "module": "a_review0_rclone_target",
  "name": "ReviewRcloneEffectTargetService",
  "unit": "export"
}
```

</details>
