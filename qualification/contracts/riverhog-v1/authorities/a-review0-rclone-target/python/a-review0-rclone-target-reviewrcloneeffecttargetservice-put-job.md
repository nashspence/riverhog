# a_review0_rclone_target.ReviewRcloneEffectTargetService.put_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-review0-rclone-target:a-review0-rclone-target-reviewrcloneeffec-b7f2675af7:c6e37f76c8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-rclone-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-21d45298c8"></a>
- <a id="s-d33d60af74"></a>`distribution`: `a-review0-rclone-target`
- <a id="s-7f6177f04f"></a>`module`: `a_review0_rclone_target`
- <a id="s-127ca2bee8"></a>`name`: `put_job`
- <a id="s-941770bda3"></a>`owner`: `a_review0_rclone_target.ReviewRcloneEffectTargetService`
- <a id="s-6e69ed9b81"></a>`unit`: `member`

### Declared structure

- <a id="s-b89b60f4a3"></a>`kind`: `"method"`
- <a id="s-5afd8c972e"></a>`signature`: `"\"(self, request: 'TargetJobRequest') -> 'TargetJobStatus'\""`

## Maintained corroboration

### Related interface records

- [ReviewRcloneEffectTargetService](a-review0-rclone-target-reviewrcloneeffecttargetservice.md)

## Governing policies

- <a id="pa-263cb44e08"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-review0-rclone-target:a_review0_rclone_target](../../../evidence/sources/authorities.md#src-b6b8161d65) — [some-implementations/stove0/review0/rclone-effect-target/src/a\_review0\_rclone\_target/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/rclone-effect-target/src/a_review0_rclone_target/__init__.py)

### Machine authority

- `/external_contract/python/a_review0_rclone_target.ReviewRcloneEffectTargetService.put_job`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d657d8430d87c665d65b05e6a964f2eba79abfabf5c283aa5951e6a8dbadb174 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'TargetJobRequest') -> 'TargetJobStatus'\""
  },
  "distribution": "a-review0-rclone-target",
  "module": "a_review0_rclone_target",
  "name": "put_job",
  "owner": "a_review0_rclone_target.ReviewRcloneEffectTargetService",
  "unit": "member"
}
```

</details>
