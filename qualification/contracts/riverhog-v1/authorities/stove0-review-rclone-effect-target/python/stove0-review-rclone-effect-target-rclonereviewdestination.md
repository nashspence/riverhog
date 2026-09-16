# stove0_review_rclone_effect_target.RcloneReviewDestination

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-rclone-effect-target:stove0-review-rclone-effect-target-rclone-ee301d544c:ad901f5e4b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-rclone-effect-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f22daed25e"></a>
- <a id="s-a9e48ccdb8"></a>`distribution`: `stove0-review-rclone-effect-target`
- <a id="s-f86ffc0684"></a>`module`: `stove0_review_rclone_effect_target`
- <a id="s-494f327aa5"></a>`name`: `RcloneReviewDestination`
- <a id="s-46721d52da"></a>`unit`: `export`

### Declared structure

- <a id="s-74d5ea8af0"></a>`kind`: `"class"`
- <a id="s-72b2448305"></a>`signature`: `"\"(identity: 'str', remote: 'str', config_path: 'Path \| None' = None, executable: 'str' = 'rclone', timeout_seconds: 'int' = 86400) -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-4ee7eecfda"></a>`identity` | `'str'` | `required` |
| <a id="s-4409d2851f"></a>`remote` | `'str'` | `required` |
| <a id="s-1ddf3348d2"></a>`config_path` | `'Path \| None'` | `None` |
| <a id="s-675c99507f"></a>`executable` | `'str'` | `'rclone'` |
| <a id="s-e09ef4d6ba"></a>`timeout_seconds` | `'int'` | `86400` |

## Maintained corroboration

### Related interface records

- [commit](stove0-review-rclone-effect-target-rclonereviewdestination-commit.md)

## Governing policies

- <a id="pa-a56ddcfc76"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-rclone-effect-target:stove0_review_rclone_effect_target](../../../evidence/sources.md#src-5fd1cb5bbe) — `reference/stove0/targets/review/rclone-effect-target/src/stove0_review_rclone_effect_target/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_rclone_effect_target.RcloneReviewDestination`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dac4def867d2aca24ac9864b87150c908b1b4b7fac4db58dcfbebe8cad9bbcfb -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "identity",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "remote",
        "type": "'str'"
      },
      {
        "default": "None",
        "name": "config_path",
        "type": "'Path | None'"
      },
      {
        "default": "'rclone'",
        "name": "executable",
        "type": "'str'"
      },
      {
        "default": "86400",
        "name": "timeout_seconds",
        "type": "'int'"
      }
    ],
    "kind": "class",
    "signature": "\"(identity: 'str', remote: 'str', config_path: 'Path | None' = None, executable: 'str' = 'rclone', timeout_seconds: 'int' = 86400) -> None\""
  },
  "distribution": "stove0-review-rclone-effect-target",
  "module": "stove0_review_rclone_effect_target",
  "name": "RcloneReviewDestination",
  "unit": "export"
}
```

</details>
