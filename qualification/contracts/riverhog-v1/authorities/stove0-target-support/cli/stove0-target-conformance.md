# stove0-target-conformance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-target-support:stove0-target-conformance:dbb297a0ce -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [cli](index.md) |
| Family | [root](index.md#f-58e39c4c6f) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- <a id="s-fde6abe43b"></a>Parser name: `stove0-target-conformance`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-e562e2016c"></a>`` | _StoreAction | yes |  |  |
| <a id="s-e11b46d794"></a>`` | _AppendAction | no | Path | --case |

## Governing policies

- <a id="pa-6e0c5ec60f"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0-target-conformance](../../../evidence/sources.md#src-7a44eec01b) — `reference/stove0/packages/target-support/src/stove0_target_support/conformance.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

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
