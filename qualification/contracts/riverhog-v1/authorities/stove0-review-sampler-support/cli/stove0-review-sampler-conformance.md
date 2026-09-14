# stove0-review-sampler-conformance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-review-sampler-support:stove0-review-sampler-conformance:13ece4b55a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-support](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-967568b4a9"></a>Parser name: `stove0-review-sampler-conformance`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-c7e1a9bf84"></a>`base_url` | _StoreAction | yes |  |  |
| <a id="s-01e4236343"></a>`token_file` | _StoreAction | yes | Path | --token-file |
| <a id="s-503c002d8f"></a>`request` | _StoreAction | no | Path | --request |
| <a id="s-e2cfd0f79c"></a>`allow_insecure_http` | _StoreTrueAction | no |  | --allow-insecure-http |
| <a id="s-d47e544d0c"></a>`version` | _VersionAction | no |  | --version |

### Result and failure contract

- <a id="s-4a89cb0fbe"></a>Result identity: `stove0-review-sampler-conformance-cli-result/root/v1`
- <a id="s-ee014073f8"></a>Profile: `stove0-review-sampler-conformance-cli/v1`
- <a id="s-1291c932e0"></a>Structured output: `always-json`
- <a id="s-ec057813d2"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-151d33ac93"></a>`conformant` | <a id="s-3193d17a65"></a>`0` | <a id="s-7b6d3f9844"></a>`{"json":"stove0-review-sampler-conformance-result/v1"}` | <a id="s-6d3f8cfee1"></a>`{"all":"empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-c3d5e2d565"></a>`usage` | <a id="s-bd61b61d40"></a>`2` | <a id="s-575d000400"></a>`{"all":"empty"}` | <a id="s-3efe152094"></a>`{"all":"noncontractual-usage-diagnostic"}` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --allow-insecure-http](#s-e2cfd0f79c) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --version](#s-d47e544d0c) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-8052e808b3"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-c408f6225d"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

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
- `/external_contract/cli/stove0-review-sampler-conformance/result_contract`

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

### `/external_contract/cli/stove0-review-sampler-conformance/result_contract`

<!-- exact-contract-value: 70d408ace59c1796138841e688904da066ca3e801bf10f85d595c0b4a46e5cc4 -->

```json
{
  "failures": [
    {
      "exit_status": 2,
      "id": "usage",
      "stderr": {
        "all": "noncontractual-usage-diagnostic"
      },
      "stdout": {
        "all": "empty"
      }
    }
  ],
  "human_json_relationship": "not-applicable",
  "identity": "stove0-review-sampler-conformance-cli-result/root/v1",
  "profile_id": "stove0-review-sampler-conformance-cli/v1",
  "structured_output": "always-json",
  "success": [
    {
      "exit_status": 0,
      "id": "conformant",
      "stderr": {
        "all": "empty"
      },
      "stdout": {
        "json": "stove0-review-sampler-conformance-result/v1"
      }
    }
  ]
}
```
