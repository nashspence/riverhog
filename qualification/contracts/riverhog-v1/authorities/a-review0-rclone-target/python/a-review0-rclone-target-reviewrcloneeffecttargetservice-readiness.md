# a_review0_rclone_target.ReviewRcloneEffectTargetService.readiness

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-review0-rclone-target:a-review0-rclone-target-reviewrcloneeffec-d14cd73092:9a79f71c49 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-rclone-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-541e93f220"></a>
- <a id="s-8c3c43a52b"></a>`distribution`: `a-review0-rclone-target`
- <a id="s-a8f11916a7"></a>`module`: `a_review0_rclone_target`
- <a id="s-f96444999a"></a>`name`: `readiness`
- <a id="s-02607b72b3"></a>`owner`: `a_review0_rclone_target.ReviewRcloneEffectTargetService`
- <a id="s-543d80f2e7"></a>`unit`: `member`

### Declared structure

- <a id="s-5dbcdeed97"></a>`kind`: `"method"`
- <a id="s-6fab1ae44f"></a>`signature`: `"\"(self) -> 'dict[str, str]'\""`

## Maintained corroboration

### Related interface records

- [ReviewRcloneEffectTargetService](a-review0-rclone-target-reviewrcloneeffecttargetservice.md)

## Governing policies

- <a id="pa-157945b60b"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-review0-rclone-target:a_review0_rclone_target](../../../evidence/sources/authorities.md#src-b6b8161d65) — [some-implementations/stove0/review0/rclone-effect-target/src/a\_review0\_rclone\_target/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/rclone-effect-target/src/a_review0_rclone_target/__init__.py)

### Machine authority

- `/external_contract/python/a_review0_rclone_target.ReviewRcloneEffectTargetService.readiness`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ea50d555a41554aa5a23a42ae009079b28cf2c28abacef0b63b4d7879f96eea6 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'dict[str, str]'\""
  },
  "distribution": "a-review0-rclone-target",
  "module": "a_review0_rclone_target",
  "name": "readiness",
  "owner": "a_review0_rclone_target.ReviewRcloneEffectTargetService",
  "unit": "member"
}
```

</details>
