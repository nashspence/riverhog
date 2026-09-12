# stove0-review-rclone-effect-target component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-review-rclone-effect-target:stove0-review-rclone-effect-target-compon-638caf4063:19957af060 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-rclone-effect-target](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-63fb220dd5ac) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-34b7b408f9b3"></a>
| Field | Shape |
|---|---|
| <a id="s-52161c7a4bda"></a>`console_scripts` | additional keys=`stove0-review-rclone-effect-target` |
| <a id="s-b6605a43d517"></a>`dependencies` | ["riverhog-client","riverhog-protocol","stove0-review-target-contracts","stove0-review-target-support","stove0-target-support"] |
| <a id="s-f9ce12e51209"></a>`distribution` | "stove0-review-rclone-effect-target" |
| <a id="s-a840a54e4e2b"></a>`optional_dependencies` | empty object |
| <a id="s-490915dd7711"></a>`path` | "reference/stove0/targets/review/rclone-effect-target" |
| <a id="s-a8d4b28a0ea0"></a>`role` | "reference_component" |

## Governing policies

- <a id="pa-7390687c5540"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0fa8)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6b1)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5fe0) — `release.toml`

### Machine authority

- `/boundaries/components/65`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4e79e8d3823950947e237347ed5551f7665fec65cc063f3edae49427861bdc5f -->

```json
{
  "console_scripts": {
    "stove0-review-rclone-effect-target": "stove0_review_rclone_effect_target.app:main"
  },
  "dependencies": [
    "riverhog-client",
    "riverhog-protocol",
    "stove0-review-target-contracts",
    "stove0-review-target-support",
    "stove0-target-support"
  ],
  "distribution": "stove0-review-rclone-effect-target",
  "optional_dependencies": {},
  "path": "reference/stove0/targets/review/rclone-effect-target",
  "role": "reference_component"
}
```
