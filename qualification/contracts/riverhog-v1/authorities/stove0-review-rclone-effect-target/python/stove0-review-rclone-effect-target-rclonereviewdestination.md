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
| Field | Shape |
|---|---|
| <a id="s-d903112e93"></a>`contract` | additional keys=`fields`, `kind`, `signature` |
| <a id="s-a9e48ccdb8"></a>`distribution` | "stove0-review-rclone-effect-target" |
| <a id="s-f86ffc0684"></a>`module` | "stove0_review_rclone_effect_target" |
| <a id="s-494f327aa5"></a>`name` | "RcloneReviewDestination" |
| <a id="s-46721d52da"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_review_rclone_effect_target.RcloneReviewDestination.commit](stove0-review-rclone-effect-target-rclonereviewdestination-commit.md)

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
