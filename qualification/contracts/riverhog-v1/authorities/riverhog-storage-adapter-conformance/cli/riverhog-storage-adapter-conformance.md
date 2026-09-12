# riverhog-storage-adapter-conformance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-storage-adapter-conformance:riverhog-storage-adapter-conformance:6cf179f1ba -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-conformance` |
| Interface | `cli` |
| Family | `root` |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

- Parser name: `riverhog-storage-adapter-conformance`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `` | _StoreAction | yes |  | --base-url |
| `` | _StoreAction | yes | Path | --token-file |
| `` | _StoreAction | yes |  | --object-prefix |
| `` | _StoreTrueAction | no |  | --allow-insecure-http |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| cardinality | values-per-occurrence | `fixed` | maximum=0, minimum=0, reason=fixed-command-argument-arity |

## Governing policies

- `compatibility/cli/v1`
- `extent-rule/schema-bound/v1`

## Evidence

### Qualification

- `make dist-smoke`
- `make operation-qualification`

### Executable sources

- `cli:riverhog-storage-adapter-conformance` — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/conformance.py::<module>`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/riverhog-storage-adapter-conformance/name`
- `/external_contract/cli/riverhog-storage-adapter-conformance/parameters`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/riverhog-storage-adapter-conformance/name`

<!-- exact-contract-value: 98d8fde2010458c2409a2fc87e33c689b1aca3836ae3f679276be44504d4ca90 -->

```json
"riverhog-storage-adapter-conformance"
```

### `/external_contract/cli/riverhog-storage-adapter-conformance/parameters`

<!-- exact-contract-value: 372bdc5b4e66958de053a3c608e89c26fd45b77348f19a9cb0ecdf6c8ac8f31f -->

```json
[
  {
    "dest": "base_url",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--base-url"
    ],
    "required": true
  },
  {
    "dest": "token_file",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--token-file"
    ],
    "required": true,
    "type": "Path"
  },
  {
    "dest": "object_prefix",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--object-prefix"
    ],
    "required": true
  },
  {
    "default": false,
    "dest": "allow_insecure_http",
    "kind": "_StoreTrueAction",
    "nargs": 0,
    "options": [
      "--allow-insecure-http"
    ],
    "required": false
  }
]
```
