# Test Only runtime images

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:repository:test-only-runtime-images:0c65aa5a96 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [repository](../index.md) |
| Interface | [boundary](index.md) |
| Family | [runtime-images](index.md#f-dfd50c0c8689) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-93f50074e0f9"></a>
| Field | Shape |
|---|---|
| <a id="s-79b6bba7a403"></a>`test` | additional keys=`local_tag` |

## Governing policies

- <a id="pa-228cefbb1315"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0fa8)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6b1)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5fe0) — `release.toml`

### Machine authority

- `/boundaries/runtime_images/test_only`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 21a3163c7b6ebde16b9d46ce3d7decd8790edd90f9e68f305425165269cdba3b -->

```json
{
  "test": {
    "local_tag": "riverhog-test:dev"
  }
}
```
