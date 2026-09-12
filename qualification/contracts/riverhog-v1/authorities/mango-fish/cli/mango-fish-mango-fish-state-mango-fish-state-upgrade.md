# mango-fish mango-fish state mango-fish state upgrade

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:mango-fish:mango-fish-mango-fish-state-mango-fish-state-upgrade:a4849199e0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [mango-fish](../index.md) |
| Interface | [cli](index.md) |
| Family | [mango-fish state](index.md#f-baa3c102f7) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

- <a id="s-1b645cf3be"></a>Parser name: `mango-fish state upgrade`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-0d0b0accba"></a>`` | _StoreTrueAction | no |  | --json |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-0d0b0accba) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-9bbe46c32e"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-397cd858f8"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:mango-fish](../../../evidence/sources.md#src-3dcd5eedf2) — `reference/riverhog/applications/mango-fish/src/mango_fish/cli.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/mango-fish/commands/state/commands/upgrade/name`
- `/external_contract/cli/mango-fish/commands/state/commands/upgrade/parameters`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/mango-fish/commands/state/commands/upgrade/name`

<!-- exact-contract-value: 6c87db9d632bfdae0883d34c415798e02d392a141213ec3a8911b509b2cc965c -->

```json
"mango-fish state upgrade"
```

### `/external_contract/cli/mango-fish/commands/state/commands/upgrade/parameters`

<!-- exact-contract-value: a4eee56605df36f2261ef19e35bdc3c16631d89a610d870dece837b2b2866441 -->

```json
[
  {
    "default": false,
    "dest": "json",
    "kind": "_StoreTrueAction",
    "nargs": 0,
    "options": [
      "--json"
    ],
    "required": false
  }
]
```
