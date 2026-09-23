# review0-sampler-conformance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:review0-sampler-lib:review0-sampler-conformance:87a8777b04 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-lib](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-b920ceebed"></a>Parser name: `review0-sampler-conformance`
- <a id="s-50342a135c"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-f68efa7f72"></a>`base_url` | required positional; 1 value | not recorded | not recorded |
| <a id="s-725dce5540"></a>`token_file`<br>`--token-file` | required option; 1 value | Path | not recorded |
| <a id="s-54ec1d29c0"></a>`request`<br>`--request` | optional option; 1 value | Path | not recorded |
| <a id="s-72800993ad"></a>`allow_insecure_http`<br>`--allow-insecure-http` | optional flag; 0 values | not recorded | `false` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-d79e1aef64"></a>`help` | <a id="s-e90c00b282"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-92a5544a3b"></a>`0` | <a id="s-16e49cfb18"></a>`"noncontractual-framework-help"` | <a id="s-316f04b93d"></a>`"empty"` |
| <a id="s-b3e27747dd"></a>`version` | <a id="s-1059ed2b8e"></a>`{"kind":"option-present","options":["--version"]}` | <a id="s-fb37d8dc71"></a>`0` | <a id="s-44a6ecacfc"></a>`{"distribution":"review0-sampler-lib","kind":"installed-coordinated-release-version","serialization":"noncontractual"}` | <a id="s-ce50c234e7"></a>`"empty"` |

### Result and failure contract

- <a id="s-cf40fb6fd8"></a>Result identity: `review0-sampler-conformance-cli-result/root/v1`
- <a id="s-f13196a223"></a>Profile: `review0-sampler-conformance-cli/v1`
- <a id="s-953e7b3601"></a>Structured output: `always-json`
- <a id="s-c626bdbbcb"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-0abed70f27"></a>`conformant` | <a id="s-009f515e59"></a>`{"kind":"conformance-completed"}` | <a id="s-19f0307e3b"></a>`0` | <a id="s-b7349e2f4a"></a>json: [generated:review0-sampler: SamplerConformanceResult](../process-protocol-schemas/generated-review0-sampler-samplerconformanceresult.md) | <a id="s-cb030ecd8a"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-60e09a7c74"></a>`usage` | <a id="s-2100676340"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-bcb90b0252"></a>`2` | <a id="s-69fe55f32b"></a>all: `"empty"` | <a id="s-d13949f9e1"></a>all: `"noncontractual-usage-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter 0](#s-f68efa7f72) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1 |
| [CLI parameter --token-file](#s-725dce5540) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1 |
| [CLI parameter --request](#s-54ec1d29c0) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1 |
| [CLI parameter --allow-insecure-http](#s-72800993ad) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0 |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-2348cdacec"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-1575ba6816"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:review0-sampler-conformance](../../../evidence/sources/authorities.md#src-8d0413fce6) — [some-implementations/stove0/review0/sampler/support/src/review0\_sampler\_lib/conformance.py::&lt;module&gt;](../../../../../../some-implementations/stove0/review0/sampler/support/src/review0_sampler_lib/conformance.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/review0-sampler-conformance/allow_abbrev`
- `/external_contract/cli/review0-sampler-conformance/name`
- `/external_contract/cli/review0-sampler-conformance/parameters`
- `/external_contract/cli/review0-sampler-conformance/result_contract`
- `/external_contract/cli/review0-sampler-conformance/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/review0-sampler-conformance/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/review0-sampler-conformance/name`

<!-- exact-contract-value: 7a290a2fff484963b0c477dc45ec9d3dcab7f4bb22ae8e364fd3ce171f337d7f -->

```json
"review0-sampler-conformance"
```

### `/external_contract/cli/review0-sampler-conformance/parameters`

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

### `/external_contract/cli/review0-sampler-conformance/result_contract`

<!-- exact-contract-value: 5fb00356b072f99fb14e3d1297962eeb7dbf3e4b5022bb264f9b830980549166 -->

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
  "identity": "review0-sampler-conformance-cli-result/root/v1",
  "profile_id": "review0-sampler-conformance-cli/v1",
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
          "authority": "generated:review0-sampler",
          "definition": "SamplerConformanceResult",
          "kind": "schema-authority"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/review0-sampler-conformance/terminating_controls`

<!-- exact-contract-value: 335df2af25083f83c73a4da9864b883123434a963b040b0355ecd13eeb669e8c -->

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
      "distribution": "review0-sampler-lib",
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
