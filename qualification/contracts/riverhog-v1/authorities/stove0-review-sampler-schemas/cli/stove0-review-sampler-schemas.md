# stove0-review-sampler-schemas

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-review-sampler-schemas:stove0-review-sampler-schemas:e0c130478c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-schemas](../index.md) |
| Interface | [cli](index.md) |
| Family | [root](index.md#f-652d17db43) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

- <a id="s-faa7bf9354"></a>Parser name: `stove0-review-sampler-schemas`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-c1a6264e75"></a>`` | _VersionAction | no |  | --version |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --version](#s-c1a6264e75) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-42c42e2209"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-b367f940ab"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0-review-sampler-schemas](../../../evidence/sources.md#src-a75c35f0c8) — `reference/stove0/targets/review/sampler/support/src/stove0_review_sampler_support/schemas.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0-review-sampler-schemas/name`
- `/external_contract/cli/stove0-review-sampler-schemas/parameters`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0-review-sampler-schemas/name`

<!-- exact-contract-value: 162a3ef3dc87aa1b3b1b2b34823d4702440ce3754ea9e31d325df3f59ec24a0c -->

```json
"stove0-review-sampler-schemas"
```

### `/external_contract/cli/stove0-review-sampler-schemas/parameters`

<!-- exact-contract-value: 280890e414521dae90aaec62414808f8ed59735cf99b00ca647a8e328fd86c02 -->

```json
[
  {
    "dest": "version",
    "kind": "_VersionAction",
    "nargs": 0,
    "options": [
      "--version"
    ],
    "required": false
  }
]
```
