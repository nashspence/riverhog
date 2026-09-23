# a_review0_rclone_target.RcloneReviewDestination.commit

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-review0-rclone-target:a-review0-rclone-target-rclonereviewdesti-79537112e8:9d123a7b32 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-rclone-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f3d71fe016"></a>
- <a id="s-53d988ea3b"></a>`distribution`: `a-review0-rclone-target`
- <a id="s-a81e552d31"></a>`module`: `a_review0_rclone_target`
- <a id="s-83036b0f62"></a>`name`: `commit`
- <a id="s-e0acc35773"></a>`owner`: `a_review0_rclone_target.RcloneReviewDestination`
- <a id="s-f3f9524ab8"></a>`unit`: `member`

### Declared structure

- <a id="s-c4fe253521"></a>`kind`: `"method"`
- <a id="s-36c4aba82e"></a>`signature`: `"\"(self, *, delivery_id: 'str', output_root: 'Path', artifacts: 'Sequence[OutputArtifact]', manifest_path: 'Path') -> 'dict[str, JsonValue]'\""`

## Maintained corroboration

### Related interface records

- [RcloneReviewDestination](a-review0-rclone-target-rclonereviewdestination.md)

## Governing policies

- <a id="pa-18dc9e391c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-review0-rclone-target:a_review0_rclone_target](../../../evidence/sources/authorities.md#src-b6b8161d65) — [some-implementations/stove0/review0/rclone-effect-target/src/a\_review0\_rclone\_target/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/rclone-effect-target/src/a_review0_rclone_target/__init__.py)

### Machine authority

- `/external_contract/python/a_review0_rclone_target.RcloneReviewDestination.commit`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 37ad84d0e7cf03018e40ca8562d2308e53aeb518329b06569ce23577db9c499f -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, delivery_id: 'str', output_root: 'Path', artifacts: 'Sequence[OutputArtifact]', manifest_path: 'Path') -> 'dict[str, JsonValue]'\""
  },
  "distribution": "a-review0-rclone-target",
  "module": "a_review0_rclone_target",
  "name": "commit",
  "owner": "a_review0_rclone_target.RcloneReviewDestination",
  "unit": "member"
}
```

</details>
