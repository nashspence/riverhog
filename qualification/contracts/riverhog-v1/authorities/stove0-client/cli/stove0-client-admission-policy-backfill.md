# stove0-client admission policy backfill

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-client-admission-policy-backfill:d2111d4fa5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [cli](index.md) |
| Family | [admission](families/admission/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

- <a id="s-d32a9028d2"></a>Parser name: `backfill`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-ad33cb5cc6"></a>`policy_id` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | policy_id |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter policy_id](#s-ad33cb5cc6) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [Operation parity: backfill_admission_policy](../../stove0/operation/operation-parity-backfill-admission-policy.md)

## Governing policies

- <a id="pa-f230abdea8"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-1d2f55244b"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources.md#src-6203ae7d88) — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0/commands/admission/commands/policy/commands/backfill/name`
- `/external_contract/cli/stove0/commands/admission/commands/policy/commands/backfill/parameters`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/admission/commands/policy/commands/backfill/name`

<!-- exact-contract-value: 598bbc355057414041935e7063530c6581ced46576ae4f50eb9a26c33752e354 -->

```json
"backfill"
```

### `/external_contract/cli/stove0/commands/admission/commands/policy/commands/backfill/parameters`

<!-- exact-contract-value: c2ba15d022e17d87c4fef6b4fc54fd47ee4a58f77e072e2fddfb95ae64e3222a -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "policy_id",
    "nargs": 1,
    "options": [
      "policy_id"
    ],
    "required": true,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.StringParamType",
      "name": "text"
    }
  }
]
```
