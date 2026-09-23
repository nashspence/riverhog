# a_review0_rclone_target.ReviewRcloneEffectTargetService.cancel_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-review0-rclone-target:a-review0-rclone-target-reviewrcloneeffec-c299415aae:33d4f802dc -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-rclone-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4d2708514e"></a>
- <a id="s-9b20fe4af7"></a>`distribution`: `a-review0-rclone-target`
- <a id="s-f0268fa352"></a>`module`: `a_review0_rclone_target`
- <a id="s-0b3cb36305"></a>`name`: `cancel_job`
- <a id="s-7a7b1b90b4"></a>`owner`: `a_review0_rclone_target.ReviewRcloneEffectTargetService`
- <a id="s-0922591d78"></a>`unit`: `member`

### Declared structure

- <a id="s-e0c5aed659"></a>`kind`: `"method"`
- <a id="s-642170c2fe"></a>`signature`: `"\"(self, job_id: 'str') -> 'TargetJobStatus'\""`

## Maintained corroboration

### Related interface records

- [ReviewRcloneEffectTargetService](a-review0-rclone-target-reviewrcloneeffecttargetservice.md)

## Governing policies

- <a id="pa-4ac72a1721"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-review0-rclone-target:a_review0_rclone_target](../../../evidence/sources/authorities.md#src-b6b8161d65) — [some-implementations/stove0/review0/rclone-effect-target/src/a\_review0\_rclone\_target/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/rclone-effect-target/src/a_review0_rclone_target/__init__.py)

### Machine authority

- `/external_contract/python/a_review0_rclone_target.ReviewRcloneEffectTargetService.cancel_job`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 16c879f7d0994d810170c3f3161bffab689c487002acb6d078d87f85dee10670 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, job_id: 'str') -> 'TargetJobStatus'\""
  },
  "distribution": "a-review0-rclone-target",
  "module": "a_review0_rclone_target",
  "name": "cancel_job",
  "owner": "a_review0_rclone_target.ReviewRcloneEffectTargetService",
  "unit": "member"
}
```

</details>
