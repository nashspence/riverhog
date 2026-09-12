# stove0-review-sampler-conformance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-review-sampler-support:stove0-review-sampler-conformance:a395b9e874 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-support](../index.md) |
| Interface | [cli](index.md) |
| Family | [root](index.md#f-a77fbe9ea1) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

- <a id="s-967568b4a9"></a>Parser name: `stove0-review-sampler-conformance`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-c7e1a9bf84"></a>`` | _StoreAction | yes |  |  |
| <a id="s-01e4236343"></a>`` | _StoreAction | yes | Path | --token-file |
| <a id="s-503c002d8f"></a>`` | _StoreAction | no | Path | --request |
| <a id="s-e2cfd0f79c"></a>`` | _StoreTrueAction | no |  | --allow-insecure-http |
| <a id="s-d47e544d0c"></a>`` | _VersionAction | no |  | --version |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --allow-insecure-http](#s-e2cfd0f79c) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --version](#s-d47e544d0c) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-d611dc9000"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-a798b3a6da"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0-review-sampler-conformance](../../../evidence/sources.md#src-5796b3dff4) — `reference/stove0/targets/review/sampler/support/src/stove0_review_sampler_support/conformance.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0-review-sampler-conformance/name`
- `/external_contract/cli/stove0-review-sampler-conformance/parameters`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0-review-sampler-conformance/name`

<!-- exact-contract-value: 2be934e3e5c6d2b8852132daa56e7eacc7a61415aa715a8e2ecb0373352b3905 -->

```json
"stove0-review-sampler-conformance"
```

### `/external_contract/cli/stove0-review-sampler-conformance/parameters`

<!-- exact-contract-value: 49cca455fdf894c8d637d0b2169d59ee01d635b1ff796a41e3b609b0e826eb3e -->

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
    "dest": "request",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--request"
    ],
    "required": false,
    "type": "Path"
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
  },
  {
    "dest": "version",
    "kind": "_VersionAction",
    "nargs": 0,
    "options": [
      "--version"
    ],
    "required": false
  }
]
```
