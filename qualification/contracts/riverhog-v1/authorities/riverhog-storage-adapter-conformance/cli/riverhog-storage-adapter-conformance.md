# riverhog-storage-adapter-conformance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-storage-adapter-conformance:riverhog-storage-adapter-conformance:6cf179f1ba -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-conformance](../index.md) |
| Interface | [cli](index.md) |
| Family | [root](index.md#f-d81688a5b8f7) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

- <a id="s-0d03ef177794"></a>Parser name: `riverhog-storage-adapter-conformance`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-da9cd0d5da4c"></a>`` | _StoreAction | yes |  | --base-url |
| <a id="s-4db4323e5a5b"></a>`` | _StoreAction | yes | Path | --token-file |
| <a id="s-76ce9fec003e"></a>`` | _StoreAction | yes |  | --object-prefix |
| <a id="s-785a5f2564ac"></a>`` | _StoreTrueAction | no |  | --allow-insecure-http |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --allow-insecure-http](#s-785a5f2564ac) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-7f4640187144"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-4cca82f579b6"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [cli:riverhog-storage-adapter-conformance](../../../evidence/sources.md#src-7ab923569bbb) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/conformance.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

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
