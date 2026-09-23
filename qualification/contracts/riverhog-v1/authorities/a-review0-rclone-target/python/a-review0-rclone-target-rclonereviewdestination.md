# a_review0_rclone_target.RcloneReviewDestination

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-review0-rclone-target:a-review0-rclone-target-rclonereviewdestination:2ae23d14a1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-rclone-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fae5c44787"></a>
- <a id="s-398ea1de5b"></a>`distribution`: `a-review0-rclone-target`
- <a id="s-ea36faeff2"></a>`module`: `a_review0_rclone_target`
- <a id="s-92f2bcbfa0"></a>`name`: `RcloneReviewDestination`
- <a id="s-485e47f0c4"></a>`unit`: `export`

### Declared structure

- <a id="s-42e3d05e68"></a>`kind`: `"class"`
- <a id="s-227a3fe817"></a>`signature`: `"\"(identity: 'str', remote: 'str', config_path: 'Path \| None' = None, executable: 'str' = 'rclone', timeout_seconds: 'int' = 86400) -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-69f2246272"></a>`identity` | `'str'` | `required` |
| <a id="s-18a7599209"></a>`remote` | `'str'` | `required` |
| <a id="s-ae0580b219"></a>`config_path` | `'Path \| None'` | `None` |
| <a id="s-5d655bf763"></a>`executable` | `'str'` | `'rclone'` |
| <a id="s-39da3f4396"></a>`timeout_seconds` | `'int'` | `86400` |

## Maintained corroboration

### Related interface records

- [commit](a-review0-rclone-target-rclonereviewdestination-commit.md)

## Governing policies

- <a id="pa-5ecba7b258"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-review0-rclone-target:a_review0_rclone_target](../../../evidence/sources/authorities.md#src-b6b8161d65) — [some-implementations/stove0/review0/rclone-effect-target/src/a\_review0\_rclone\_target/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/rclone-effect-target/src/a_review0_rclone_target/__init__.py)

### Machine authority

- `/external_contract/python/a_review0_rclone_target.RcloneReviewDestination`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9ae7d3c18fa91f350c54ed5f9ab68acd672551ec9b3207d1c24d780704444806 -->

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
  "distribution": "a-review0-rclone-target",
  "module": "a_review0_rclone_target",
  "name": "RcloneReviewDestination",
  "unit": "export"
}
```

</details>
