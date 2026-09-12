# mango-fish mango-fish state mango-fish state status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:mango-fish:mango-fish-mango-fish-state-mango-fish-state-status:cb951bc12c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [mango-fish](../index.md) |
| Interface | [cli](index.md) |
| Family | [mango-fish state](index.md#f-baa3c102f7) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

- <a id="s-23c16a2129"></a>Parser name: `mango-fish state status`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-f1aea638f4"></a>`` | _StoreTrueAction | no |  | --json |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-f1aea638f4) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-411afeb2a0"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-4bcd971c29"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:mango-fish](../../../evidence/sources.md#src-3dcd5eedf2) — `reference/riverhog/applications/mango-fish/src/mango_fish/cli.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/mango-fish/commands/state/commands/status/name`
- `/external_contract/cli/mango-fish/commands/state/commands/status/parameters`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/mango-fish/commands/state/commands/status/name`

<!-- exact-contract-value: 1cc2fe0b23d7dbab4c28e0a8196a53c831740180020d67a4bb7c465a5bb0cb65 -->

```json
"mango-fish state status"
```

### `/external_contract/cli/mango-fish/commands/state/commands/status/parameters`

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
