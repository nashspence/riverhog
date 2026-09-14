# stove0-review-sampler-conformance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-review-sampler-support:stove0-review-sampler-conformance:b269c354a7 -->

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

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-f464182d29"></a>`help` | <a id="s-ea22cb186a"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-3ddc673f3d"></a>`0` | <a id="s-ff99577988"></a>`"noncontractual-framework-help"` | <a id="s-4fb8d54db3"></a>`"empty"` |
| <a id="s-f683cfb357"></a>`version` | <a id="s-aee23356d2"></a>`{"kind":"option-present","options":["--version"]}` | <a id="s-164c44e397"></a>`0` | <a id="s-e2dcc98bc4"></a>`{"distribution":"stove0-review-sampler-support","kind":"installed-coordinated-release-version","serialization":"noncontractual"}` | <a id="s-97672c7a46"></a>`"empty"` |

### Result and failure contract

- <a id="s-4a89cb0fbe"></a>Result identity: `stove0-review-sampler-conformance-cli-result/root/v1`
- <a id="s-ee014073f8"></a>Profile: `stove0-review-sampler-conformance-cli/v1`
- <a id="s-1291c932e0"></a>Structured output: `always-json`
- <a id="s-ec057813d2"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-151d33ac93"></a>`conformant` | <a id="s-b8d3fee4b1"></a>`{"kind":"conformance-completed"}` | <a id="s-3193d17a65"></a>`0` | <a id="s-7b6d3f9844"></a>`json: stove0-review-sampler-conformance-result/v1` | <a id="s-6d3f8cfee1"></a>`all: empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-c3d5e2d565"></a>`usage` | <a id="s-fa2c1ebbd0"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-bd61b61d40"></a>`2` | <a id="s-575d000400"></a>`all: empty` | <a id="s-3efe152094"></a>`all: noncontractual-usage-diagnostic` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --allow-insecure-http](#s-e2cfd0f79c) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-90c9f9e548"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-c791e78a6b"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

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
- `/external_contract/cli/stove0-review-sampler-conformance/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0-review-sampler-conformance/name`

<!-- exact-contract-value: 2be934e3e5c6d2b8852132daa56e7eacc7a61415aa715a8e2ecb0373352b3905 -->

```json
"stove0-review-sampler-conformance"
```

### `/external_contract/cli/stove0-review-sampler-conformance/parameters`

<!-- exact-contract-value: 9a711e5897766dcd0b70e1b83b962fc2abfdd6b507d91f68efe5207a2e88f70d -->

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
  }
]
```

### `/external_contract/cli/stove0-review-sampler-conformance/result_contract`

<!-- exact-contract-value: b796fd56b678d5fde4b11ff6fbc727c3d0693f185aa1bde77de00fd0f293acf8 -->

```json
{
  "failures": [
    {
      "exit_status": 2,
      "id": "usage",
      "selected_by": {
        "kind": "parser-rejected-invocation"
      },
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
      "selected_by": {
        "kind": "conformance-completed"
      },
      "stderr": {
        "all": "empty"
      },
      "stdout": {
        "json": {
          "identity": "stove0-review-sampler-conformance-result/v1",
          "kind": "semantic-format"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/stove0-review-sampler-conformance/terminating_controls`

<!-- exact-contract-value: fcddd733b548f7fdee3021b986cc2c4b7650c7a1a689657a12af45cc7a2137de -->

```json
[
  {
    "exit_status": 0,
    "id": "help",
    "stderr": "empty",
    "stdout": "noncontractual-framework-help",
    "trigger": {
      "kind": "option-present",
      "options": [
        "-h",
        "--help"
      ]
    }
  },
  {
    "exit_status": 0,
    "id": "version",
    "stderr": "empty",
    "stdout": {
      "distribution": "stove0-review-sampler-support",
      "kind": "installed-coordinated-release-version",
      "serialization": "noncontractual"
    },
    "trigger": {
      "kind": "option-present",
      "options": [
        "--version"
      ]
    }
  }
]
```
