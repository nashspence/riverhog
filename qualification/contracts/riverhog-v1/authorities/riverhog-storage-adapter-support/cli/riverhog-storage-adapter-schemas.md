# riverhog-storage-adapter-schemas

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-storage-adapter-support:riverhog-storage-adapter-schemas:fbcfbe3901 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [cli](index.md) |
| Family | [root](index.md#f-4b13a4bf9b) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

- <a id="s-1ea2979964"></a>Parser name: `riverhog-storage-adapter-schemas`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-c7cd964cec"></a>`` | _StoreAction | no | Path | --output |
| <a id="s-38128e6046"></a>`` | _StoreTrueAction | no |  | --compact |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --compact](#s-38128e6046) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-d01fe2b079"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-514b9bbb23"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:riverhog-storage-adapter-schemas](../../../evidence/sources.md#src-b90a9d08ff) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/riverhog-storage-adapter-schemas/name`
- `/external_contract/cli/riverhog-storage-adapter-schemas/parameters`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/riverhog-storage-adapter-schemas/name`

<!-- exact-contract-value: 5d3570bf51282d4e0e38219724ecae45dc42a71b7197217815a8e82244b0a3d4 -->

```json
"riverhog-storage-adapter-schemas"
```

### `/external_contract/cli/riverhog-storage-adapter-schemas/parameters`

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
