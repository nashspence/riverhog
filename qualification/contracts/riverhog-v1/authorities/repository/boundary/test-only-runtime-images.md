# Test Only runtime images

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:repository:test-only-runtime-images:0c65aa5a96 -->

| Audit field | Value |
|---|---|
| Authority | `repository` |
| Interface | `boundary` |
| Family | `runtime-images` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/runtime_images/test_only`

## Effective policies

- `boundary/frozen-authority/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `release:release.toml` — `release.toml`
- Proof: `make release-check`
- Proof: `make build`

## Contract summary

| Field | Shape |
|---|---|
| `test` | object (1 fields) |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 21a3163c7b6ebde16b9d46ce3d7decd8790edd90f9e68f305425165269cdba3b -->

```json
{
  "test": {
    "local_tag": "riverhog-test:dev"
  }
}
```
