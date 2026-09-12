# stove0-target-conformance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-target-conformance:stove0-target-conformance:f16077bd02 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0-target-conformance` |
| Interface | `cli` |
| Family | `root` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- Parser name: `stove0-target-conformance`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `` | _StoreAction | yes |  |  |
| `` | _AppendAction | no | Path | --case |

## Governing policies

- `compatibility/cli/v1`

## Evidence

### Qualification

- `make dist-smoke`
- `make operation-qualification`

### Executable sources

- `cli:stove0-target-conformance` — `reference/stove0/packages/target-support/src/stove0_target_support/conformance.py::<module>`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0-target-conformance/name`
- `/external_contract/cli/stove0-target-conformance/parameters`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0-target-conformance/name`

<!-- exact-contract-value: 7d2f3eb433195f93988d5c7dcbf7137865b0f1e5c179664a7c5c265cb50162aa -->

```json
"stove0-target-conformance"
```

### `/external_contract/cli/stove0-target-conformance/parameters`

<!-- exact-contract-value: ea7099ca96775fc4d87b2bb319c3f57a9777a35ab2c40592b9644eb27f9ab03b -->

```json
[
  {
    "dest": "base_url",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [],
    "required": true
  },
  {
    "default": [],
    "dest": "case",
    "kind": "_AppendAction",
    "nargs": null,
    "options": [
      "--case"
    ],
    "required": false,
    "type": "Path"
  }
]
```
