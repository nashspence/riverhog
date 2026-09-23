# a_review0_rclone_target.ReviewRcloneEffectTargetService.get_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-review0-rclone-target:a-review0-rclone-target-reviewrcloneeffec-f6bf07d692:038ef8eeb2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-rclone-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9ba5a3f301"></a>
- <a id="s-8d5033a817"></a>`distribution`: `a-review0-rclone-target`
- <a id="s-de463afe8d"></a>`module`: `a_review0_rclone_target`
- <a id="s-3e65b0b5c9"></a>`name`: `get_job`
- <a id="s-17fa0e2aff"></a>`owner`: `a_review0_rclone_target.ReviewRcloneEffectTargetService`
- <a id="s-cfe0123524"></a>`unit`: `member`

### Declared structure

- <a id="s-265923b51f"></a>`kind`: `"method"`
- <a id="s-b9f438b0f8"></a>`signature`: `"\"(self, job_id: 'str') -> 'TargetJobStatus'\""`

## Maintained corroboration

### Related interface records

- [ReviewRcloneEffectTargetService](a-review0-rclone-target-reviewrcloneeffecttargetservice.md)

## Governing policies

- <a id="pa-7d41538b70"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-review0-rclone-target:a_review0_rclone_target](../../../evidence/sources/authorities.md#src-b6b8161d65) — [some-implementations/stove0/review0/rclone-effect-target/src/a\_review0\_rclone\_target/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/rclone-effect-target/src/a_review0_rclone_target/__init__.py)

### Machine authority

- `/external_contract/python/a_review0_rclone_target.ReviewRcloneEffectTargetService.get_job`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 76ebf7e287ee363555797a49b56ce32a30498b4fd7d8a60f7e978eff4e1b6c4f -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, job_id: 'str') -> 'TargetJobStatus'\""
  },
  "distribution": "a-review0-rclone-target",
  "module": "a_review0_rclone_target",
  "name": "get_job",
  "owner": "a_review0_rclone_target.ReviewRcloneEffectTargetService",
  "unit": "member"
}
```

</details>
