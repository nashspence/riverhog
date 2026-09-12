# stove0-observer-schemas

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-observer-schemas:stove0-observer-schemas:5ada40dcb8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-schemas](../index.md) |
| Interface | [cli](index.md) |
| Family | [root](index.md#f-c6ec7ef67392) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

- <a id="s-b8d24568a34e"></a>Parser name: `stove0-observer-schemas`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-6c05c76d4dd5"></a>`` | _StoreAction | no | Path | --output |
| <a id="s-6ff6ce676174"></a>`` | _StoreTrueAction | no |  | --compact |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --compact](#s-6ff6ce676174) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-2914af8c2886"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-4d1a669ba10b"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [cli:stove0-observer-schemas](../../../evidence/sources.md#src-e6175e3ae267) — `reference/stove0/packages/observer-support/src/stove0_observer_support/schemas.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0-observer-schemas/name`
- `/external_contract/cli/stove0-observer-schemas/parameters`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0-observer-schemas/name`

<!-- exact-contract-value: 39b4b08f24b8bf64cfcb76e91c23b866c91276b8e46a6d44e0a83af9f5f646e3 -->

```json
"stove0-observer-schemas"
```

### `/external_contract/cli/stove0-observer-schemas/parameters`

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
