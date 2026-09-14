# stove0-review-sampler-schemas

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-review-sampler-support:stove0-review-sampler-schemas:5110236e9e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-support](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-faa7bf9354"></a>Parser name: `stove0-review-sampler-schemas`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-c1a6264e75"></a>`version` | _VersionAction | no |  | --version |

### Result and failure contract

- <a id="s-4b5a5f62bd"></a>Result identity: `stove0-review-sampler-schemas-cli-result/root/v1`
- <a id="s-3201b6e3d1"></a>Profile: `stove0-review-sampler-schemas-cli/v1`
- <a id="s-1a4eacf428"></a>Structured output: `always-json`
- <a id="s-f3ff1fa2df"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-a6da464362"></a>`emitted` | <a id="s-9c0954e1dc"></a>`0` | <a id="s-fc7431ee35"></a>`{"json":"stove0-review-sampler-schema-bundle/v1"}` | <a id="s-089e853279"></a>`{"all":"empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-f8dd902aa6"></a>`usage` | <a id="s-14846d56db"></a>`2` | <a id="s-682c9abb41"></a>`{"all":"empty"}` | <a id="s-5f0da7803b"></a>`{"all":"noncontractual-usage-diagnostic"}` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --version](#s-c1a6264e75) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-b1fe8de037"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-71932e5f74"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0-review-sampler-schemas](../../../evidence/sources.md#src-a75c35f0c8) — `reference/stove0/targets/review/sampler/support/src/stove0_review_sampler_support/schemas.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0-review-sampler-schemas/name`
- `/external_contract/cli/stove0-review-sampler-schemas/parameters`
- `/external_contract/cli/stove0-review-sampler-schemas/result_contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0-review-sampler-schemas/name`

<!-- exact-contract-value: 162a3ef3dc87aa1b3b1b2b34823d4702440ce3754ea9e31d325df3f59ec24a0c -->

```json
"stove0-review-sampler-schemas"
```

### `/external_contract/cli/stove0-review-sampler-schemas/parameters`

<!-- exact-contract-value: 280890e414521dae90aaec62414808f8ed59735cf99b00ca647a8e328fd86c02 -->

```json
[
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

### `/external_contract/cli/stove0-review-sampler-schemas/result_contract`

<!-- exact-contract-value: 50f7993defb4944a661bb8899c2417fa2d0cfc371c0eadce634f46d2a544a012 -->

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
  "identity": "stove0-review-sampler-schemas-cli-result/root/v1",
  "profile_id": "stove0-review-sampler-schemas-cli/v1",
  "structured_output": "always-json",
  "success": [
    {
      "exit_status": 0,
      "id": "emitted",
      "stderr": {
        "all": "empty"
      },
      "stdout": {
        "json": "stove0-review-sampler-schema-bundle/v1"
      }
    }
  ]
}
```
