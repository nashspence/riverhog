# a_review0_rclone_target.ReviewRcloneEffectTargetService.preflight

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-review0-rclone-target:a-review0-rclone-target-reviewrcloneeffec-b081bf737c:562f22ced6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-rclone-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-315291e318"></a>
- <a id="s-6e095caf32"></a>`distribution`: `a-review0-rclone-target`
- <a id="s-764c5bef4f"></a>`module`: `a_review0_rclone_target`
- <a id="s-d17cc9f069"></a>`name`: `preflight`
- <a id="s-d011a5126b"></a>`owner`: `a_review0_rclone_target.ReviewRcloneEffectTargetService`
- <a id="s-8047bbf296"></a>`unit`: `member`

### Declared structure

- <a id="s-e61de0409a"></a>`kind`: `"method"`
- <a id="s-6dce4576be"></a>`signature`: `"\"(self, request: 'TargetPreflightRequest') -> 'TargetPreflightResponse'\""`

## Maintained corroboration

### Related interface records

- [ReviewRcloneEffectTargetService](a-review0-rclone-target-reviewrcloneeffecttargetservice.md)

## Governing policies

- <a id="pa-f6f185db55"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-review0-rclone-target:a_review0_rclone_target](../../../evidence/sources/authorities.md#src-b6b8161d65) — [some-implementations/stove0/review0/rclone-effect-target/src/a\_review0\_rclone\_target/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/rclone-effect-target/src/a_review0_rclone_target/__init__.py)

### Machine authority

- `/external_contract/python/a_review0_rclone_target.ReviewRcloneEffectTargetService.preflight`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 029dba851e67950c434519f87289ed4c3929edcb955b2f18da8bba923e506cef -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'TargetPreflightRequest') -> 'TargetPreflightResponse'\""
  },
  "distribution": "a-review0-rclone-target",
  "module": "a_review0_rclone_target",
  "name": "preflight",
  "owner": "a_review0_rclone_target.ReviewRcloneEffectTargetService",
  "unit": "member"
}
```

</details>
