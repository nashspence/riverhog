# stove0-review-sampler-conformance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-review-sampler-support:stove0-review-sampler-conformance:cae0dff9c9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-support](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-967568b4a9"></a>Parser name: `stove0-review-sampler-conformance`
- <a id="s-1c0e628ec7"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-c7e1a9bf84"></a>`base_url` | required positional; 1 value | not recorded | not recorded |
| <a id="s-01e4236343"></a>`token_file`<br>`--token-file` | required option; 1 value | Path | not recorded |
| <a id="s-503c002d8f"></a>`request`<br>`--request` | optional option; 1 value | Path | not recorded |
| <a id="s-e2cfd0f79c"></a>`allow_insecure_http`<br>`--allow-insecure-http` | optional flag; 0 values | not recorded | `false` |

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
| <a id="s-151d33ac93"></a>`conformant` | <a id="s-b8d3fee4b1"></a>`{"kind":"conformance-completed"}` | <a id="s-3193d17a65"></a>`0` | <a id="s-7b6d3f9844"></a>json: [generated:stove0-review-sampler: SamplerConformanceResult](../process-protocol-schemas/generated-stove0-review-sampler-samplerconformanceresult.md) | <a id="s-6d3f8cfee1"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-c3d5e2d565"></a>`usage` | <a id="s-fa2c1ebbd0"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-bd61b61d40"></a>`2` | <a id="s-575d000400"></a>all: `"empty"` | <a id="s-3efe152094"></a>all: `"noncontractual-usage-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter 0](#s-c7e1a9bf84) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1 |
| [CLI parameter --token-file](#s-01e4236343) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1 |
| [CLI parameter --request](#s-503c002d8f) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1 |
| [CLI parameter --allow-insecure-http](#s-e2cfd0f79c) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0 |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-9c0ca7f8a1"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-a306315237"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:stove0-review-sampler-conformance](../../../evidence/sources/authorities.md#src-5796b3dff4) — [reference/stove0/targets/review/sampler/support/src/stove0\_review\_sampler\_support/conformance.py::&lt;module&gt;](../../../../../../reference/stove0/targets/review/sampler/support/src/stove0_review_sampler_support/conformance.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/stove0-review-sampler-conformance/allow_abbrev`
- `/external_contract/cli/stove0-review-sampler-conformance/name`
- `/external_contract/cli/stove0-review-sampler-conformance/parameters`
- `/external_contract/cli/stove0-review-sampler-conformance/result_contract`
- `/external_contract/cli/stove0-review-sampler-conformance/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0-review-sampler-conformance/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

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

<!-- exact-contract-value: fbf0f4f0f558bdfda8ef9d4363b1e9acc339c832149c676d78f9bb50554ef19d -->

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
          "authority": "generated:stove0-review-sampler",
          "definition": "SamplerConformanceResult",
          "kind": "schema-authority"
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

</details>
