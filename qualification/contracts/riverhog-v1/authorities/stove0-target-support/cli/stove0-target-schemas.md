# stove0-target-schemas

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-target-support:stove0-target-schemas:317cd04861 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [cli](index.md) |
| Family | [root](index.md#f-58e39c4c6f) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

- <a id="s-bf1e05e507"></a>Parser name: `stove0-target-schemas`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-2ed9764c8b"></a>`` | _StoreAction | no | Path | --output |
| <a id="s-e30f06cf81"></a>`` | _StoreTrueAction | no |  | --compact |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --compact](#s-e30f06cf81) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-2be0084cf6"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-8ca514e9be"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0-target-schemas](../../../evidence/sources.md#src-71d64b87b5) — `reference/stove0/packages/target-support/src/stove0_target_support/schemas.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0-target-schemas/name`
- `/external_contract/cli/stove0-target-schemas/parameters`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0-target-schemas/name`

<!-- exact-contract-value: 061cab384f391f332f2ed0807ef95bc47f0507c010da1026b316ff00f4fab52b -->

```json
"stove0-target-schemas"
```

### `/external_contract/cli/stove0-target-schemas/parameters`

<!-- exact-contract-value: d09dd1e324eea493304f34cc58b3f5fe965bfdb99ef580493ab2779a9afcca7d -->

```json
[
  {
    "dest": "output",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--output"
    ],
    "required": false,
    "type": "Path"
  },
  {
    "default": false,
    "dest": "compact",
    "kind": "_StoreTrueAction",
    "nargs": 0,
    "options": [
      "--compact"
    ],
    "required": false
  }
]
```
