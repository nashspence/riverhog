# review0-planner

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:review0-planner:review0-planner:3852c1d954 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-planner](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-81809eb90f"></a>Parser name: `review0-planner`

| Field | Value |
|---|---|
| <a id="s-5a2890c255"></a>`parameters` | `[]` |
- <a id="s-1c7e79a756"></a>Unique long-option abbreviations: accepted.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-880e8bac71"></a>`help` | <a id="s-b83a059a1e"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-ac06a37843"></a>`0` | <a id="s-e2af1a3187"></a>`"noncontractual-framework-help"` | <a id="s-d6984fd6ab"></a>`"empty"` |

### Result and failure contract

- <a id="s-bc0f62e806"></a>Result identity: `review0-planner-cli-result/root/v1`
- <a id="s-68017e4685"></a>Profile: `review0-planner-cli/v1`
- <a id="s-8f4c231885"></a>Structured output: `always-json`
- <a id="s-c98e966894"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-649b6327df"></a>`reported` | <a id="s-796b3a97dc"></a>`{"kind":"contract-report-completed"}` | <a id="s-5cbaa8df6f"></a>`0` | <a id="s-068d89b000"></a>json: [review0-contract-report/v1](#s-7c3cbefea0) | <a id="s-5f803b5698"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-2917c64c16"></a>`usage` | <a id="s-32ba49f393"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-1f393ad26d"></a>`2` | <a id="s-99cebb7549"></a>all: `"empty"` | <a id="s-c8f0bf56d9"></a>all: `"noncontractual-usage-diagnostic"` |

### Local structured outputs


#### <a id="s-7c3cbefea0"></a>`review0-contract-report/v1`

Applies to: reported · stdout (json).

<a id="s-f326dc8b81"></a>

| Field | Value |
|---|---|
| <a id="s-86ff022149"></a>`format` | `"review0-contract-report/v1"` |
| <a id="s-a2bc0318da"></a>`observer_contract · contract_sha256` | `"406dee0d01ed49e29d6b7738a89607d815a256473797711952628314ffe5e79a"` |
| <a id="s-64fe0049d8"></a>`observer_contract · facts_schema · dialect` | `"https://json-schema.org/draft/2020-12/schema"` |
| <a id="s-c7851ecef0"></a>`observer_contract · facts_schema · document · $schema` | `"https://json-schema.org/draft/2020-12/schema"` |
| <a id="s-4bf48384d5"></a>`observer_contract · facts_schema · document · additionalProperties` | `false` |
| <a id="s-25280c6df3"></a>`observer_contract · facts_schema · document · properties · artifacts · items · additionalProperties` | `false` |
| <a id="s-4916007077"></a>`observer_contract · facts_schema · document · properties · artifacts · items · properties · artifact_id · minLength` | `1` |
| <a id="s-1c861a1cc0"></a>`observer_contract · facts_schema · document · properties · artifacts · items · properties · artifact_id · type` | `"string"` |
| <a id="s-2d3cbe5d43"></a>`observer_contract · facts_schema · document · properties · artifacts · items · properties · duration_ms · minimum` | `1` |
| <a id="s-fccaa1e6df"></a>`observer_contract · facts_schema · document · properties · artifacts · items · properties · duration_ms · type` | `"integer"` |
| <a id="s-9a780e3251"></a>`observer_contract · facts_schema · document · properties · artifacts · items · properties · sampleable_ranges · items · additionalProperties` | `false` |
| <a id="s-1fb02a7994"></a>`observer_contract · facts_schema · document · properties · artifacts · items · properties · sampleable_ranges · items · properties · duration_ms · minimum` | `1` |
| <a id="s-789ae70810"></a>`observer_contract · facts_schema · document · properties · artifacts · items · properties · sampleable_ranges · items · properties · duration_ms · type` | `"integer"` |
| <a id="s-c693c49eae"></a>`observer_contract · facts_schema · document · properties · artifacts · items · properties · sampleable_ranges · items · properties · start_ms · minimum` | `0` |
| <a id="s-d6362a13ef"></a>`observer_contract · facts_schema · document · properties · artifacts · items · properties · sampleable_ranges · items · properties · start_ms · type` | `"integer"` |
| <a id="s-e0e7d1d0b4"></a>`observer_contract · facts_schema · document · properties · artifacts · items · properties · sampleable_ranges · items · required` | `["start_ms","duration_ms"]` |
| <a id="s-a5e2286d8d"></a>`observer_contract · facts_schema · document · properties · artifacts · items · properties · sampleable_ranges · items · type` | `"object"` |
| <a id="s-b9e41d04c0"></a>`observer_contract · facts_schema · document · properties · artifacts · items · properties · sampleable_ranges · minItems` | `1` |
| <a id="s-7169faa809"></a>`observer_contract · facts_schema · document · properties · artifacts · items · properties · sampleable_ranges · type` | `"array"` |
| <a id="s-e7a4ff50a5"></a>`observer_contract · facts_schema · document · properties · artifacts · items · required` | `["artifact_id","duration_ms","sampleable_ranges"]` |
| <a id="s-0b61d6f85b"></a>`observer_contract · facts_schema · document · properties · artifacts · items · type` | `"object"` |
| <a id="s-78e5ee3fab"></a>`observer_contract · facts_schema · document · properties · artifacts · minItems` | `1` |
| <a id="s-5cb9bb4377"></a>`observer_contract · facts_schema · document · properties · artifacts · type` | `"array"` |
| <a id="s-5d6d5b3314"></a>`observer_contract · facts_schema · document · required` | `["artifacts"]` |
| <a id="s-8f0323d7f1"></a>`observer_contract · facts_schema · document · type` | `"object"` |
| <a id="s-46cd4817b0"></a>`observer_contract · facts_schema · format_policy` | `"annotation-only"` |
| <a id="s-8a22b7dd0c"></a>`observer_contract · facts_schema · id` | `"stove0.review.media-sampling-facts/v1"` |
| <a id="s-b13fd9e231"></a>`observer_contract · facts_schema · profile_sha256` | `"730e1fcd4a831bad9707814fe65fb59861359795bcf336ed65f72670c725f71a"` |
| <a id="s-370631d709"></a>`observer_contract · facts_semantics · conformance_vectors_sha256` | `"6f6c2bc816116299fd5ad0dc3a8e1221c0058df224062677895ad2e06325f7e8"` |
| <a id="s-bc5745af5d"></a>`observer_contract · facts_semantics · id` | `"stove0.review.media-sampling-facts-semantics/v1"` |
| <a id="s-3fe65bbe29"></a>`observer_contract · facts_semantics · profile_sha256` | `"ba56ae95653ee6d82e0505ca5b8794335f6e320edfe8395918a448c5861363da"` |
| <a id="s-6758c1fc56"></a>`observer_contract · facts_semantics · rules` | `["stove0.review.media-sampling-facts.complete-request-subjects/v1","stove0.review.media-sampling-facts.nonoverlapping-bounded-ranges/v1"]` |
| <a id="s-5a52131c7e"></a>`observer_contract · id` | `"stove0.review.media-sampling/v1"` |
| <a id="s-f38b3d48b2"></a>`observer_contract · maximum_result_bytes` | `262144` |
| <a id="s-0c7ce7ad10"></a>`observer_contract · options_schema · dialect` | `"https://json-schema.org/draft/2020-12/schema"` |
| <a id="s-492c7ed531"></a>`observer_contract · options_schema · document · $schema` | `"https://json-schema.org/draft/2020-12/schema"` |
| <a id="s-0f829e635a"></a>`observer_contract · options_schema · document · additionalProperties` | `false` |
| <a id="s-9aa969728b"></a>`observer_contract · options_schema · document · properties` | `{}` |
| <a id="s-9ba9daa965"></a>`observer_contract · options_schema · document · type` | `"object"` |
| <a id="s-d1f5a6b2c9"></a>`observer_contract · options_schema · format_policy` | `"annotation-only"` |
| <a id="s-213b93cc1e"></a>`observer_contract · options_schema · id` | `"stove0.review.media-sampling-options/v1"` |
| <a id="s-34f16418ed"></a>`observer_contract · options_schema · profile_sha256` | `"9c523e70f370789ff20984ca7a638b47ada230c1d751869ce8dfc162db15b77a"` |
| <a id="s-e2fdd71d23"></a>`operation_contract · contract_sha256` | `"7d37787e93f61be7907eec95987cec02ab108d811c34b32c59a47ecd406c5efb"` |
| <a id="s-86ea3ee5ed"></a>`operation_contract · effect_receipt_schema` | `null` |
| <a id="s-85d7a26dc0"></a>`operation_contract · id` | `"stove0.review.materialize/v1"` |
| <a id="s-f103dd016a"></a>`operation_contract · inputs · item 1 · allowed_dispositions` | `["preserved","transformed"]` |
| <a id="s-8d4e09ff2d"></a>`operation_contract · inputs · item 1 · maximum` | `null` |
| <a id="s-d3ad1aa99d"></a>`operation_contract · inputs · item 1 · minimum` | `1` |
| <a id="s-22adcc599d"></a>`operation_contract · inputs · item 1 · role` | `"stove0.review.source/v1"` |
| <a id="s-a0844f69ba"></a>`operation_contract · intent_schema · dialect` | `"https://json-schema.org/draft/2020-12/schema"` |
| <a id="s-5c3f07b118"></a>`operation_contract · intent_schema · document · $defs · JsonValue` | `{}` |
| <a id="s-cd66daa7b4"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · additionalProperties` | `false` |
| <a id="s-f94d04157e"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · properties · format · const` | `"review0-sample-plan/v1"` |
| <a id="s-bf67cdece4"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · properties · format · default` | `"review0-sample-plan/v1"` |
| <a id="s-41f378106e"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · properties · format · title` | `"Format"` |
| <a id="s-2c1797225a"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · properties · format · type` | `"string"` |
| <a id="s-83f8bb5994"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · properties · sample_plan_sha256 · pattern` | `"^[0-9a-f]{64}$"` |
| <a id="s-53ec4f3f7e"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · properties · sample_plan_sha256 · title` | `"Sample Plan Sha256"` |
| <a id="s-543078793c"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · properties · sample_plan_sha256 · type` | `"string"` |
| <a id="s-8a8965881d"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · properties · samples_per_artifact · minimum` | `1` |
| <a id="s-f13c91afcd"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · properties · samples_per_artifact · title` | `"Samples Per Artifact"` |
| <a id="s-c9d338b67a"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · properties · samples_per_artifact · type` | `"integer"` |
| <a id="s-c2d733596d"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · properties · selection_method · const` | `"evenly-spaced/v1"` |
| <a id="s-2fff8df1f4"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · properties · selection_method · default` | `"evenly-spaced/v1"` |
| <a id="s-2942761693"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · properties · selection_method · title` | `"Selection Method"` |
| <a id="s-12f7013f3b"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · properties · selection_method · type` | `"string"` |
| <a id="s-f073a74c10"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · properties · window_duration_ms · minimum` | `1` |
| <a id="s-950bd3fdac"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · properties · window_duration_ms · title` | `"Window Duration Ms"` |
| <a id="s-1c3a8c9008"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · properties · window_duration_ms · type` | `"integer"` |
| <a id="s-0167cf3638"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · properties · windows · items · $ref` | `"#/$defs/ReviewSampleWindow"` |
| <a id="s-a4b025fdbb"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · properties · windows · minItems` | `1` |
| <a id="s-e275abbcb1"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · properties · windows · title` | `"Windows"` |
| <a id="s-846f7dea6f"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · properties · windows · type` | `"array"` |
| <a id="s-770d7488a9"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · required` | `["samples_per_artifact","window_duration_ms","windows","sample_plan_sha256"]` |
| <a id="s-a88d150606"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · title` | `"ReviewSamplePlan"` |
| <a id="s-c0f830e103"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · type` | `"object"` |
| <a id="s-dd704d759b"></a>`operation_contract · intent_schema · document · $defs · ReviewSampleWindow · additionalProperties` | `false` |
| <a id="s-1d256cd456"></a>`operation_contract · intent_schema · document · $defs · ReviewSampleWindow · properties · artifact_id · maxLength` | `160` |
| <a id="s-bcd6dfd7af"></a>`operation_contract · intent_schema · document · $defs · ReviewSampleWindow · properties · artifact_id · minLength` | `1` |
| <a id="s-fe529cdd27"></a>`operation_contract · intent_schema · document · $defs · ReviewSampleWindow · properties · artifact_id · title` | `"Artifact Id"` |
| <a id="s-9fd28ac80e"></a>`operation_contract · intent_schema · document · $defs · ReviewSampleWindow · properties · artifact_id · type` | `"string"` |
| <a id="s-001ec4b388"></a>`operation_contract · intent_schema · document · $defs · ReviewSampleWindow · properties · duration_ms · minimum` | `1` |
| <a id="s-ad9d7f6ee5"></a>`operation_contract · intent_schema · document · $defs · ReviewSampleWindow · properties · duration_ms · title` | `"Duration Ms"` |
| <a id="s-0b876f4b12"></a>`operation_contract · intent_schema · document · $defs · ReviewSampleWindow · properties · duration_ms · type` | `"integer"` |
| <a id="s-d189856e9f"></a>`operation_contract · intent_schema · document · $defs · ReviewSampleWindow · properties · start_ms · minimum` | `0` |
| <a id="s-bf5b162cbd"></a>`operation_contract · intent_schema · document · $defs · ReviewSampleWindow · properties · start_ms · title` | `"Start Ms"` |
| <a id="s-9a6d818866"></a>`operation_contract · intent_schema · document · $defs · ReviewSampleWindow · properties · start_ms · type` | `"integer"` |
| <a id="s-813d3fe493"></a>`operation_contract · intent_schema · document · $defs · ReviewSampleWindow · required` | `["artifact_id","start_ms","duration_ms"]` |
| <a id="s-7ef475037e"></a>`operation_contract · intent_schema · document · $defs · ReviewSampleWindow · title` | `"ReviewSampleWindow"` |
| <a id="s-ce7d4ecb84"></a>`operation_contract · intent_schema · document · $defs · ReviewSampleWindow · type` | `"object"` |
| <a id="s-145eaff78e"></a>`operation_contract · intent_schema · document · $defs · ReviewVariantIntent · additionalProperties` | `false` |
| <a id="s-dee3541272"></a>`operation_contract · intent_schema · document · $defs · ReviewVariantIntent · properties · id · pattern` | `"^[a-z0-9]&#40;?:[a-z0-9._-]{0,158}[a-z0-9])?$"` |
| <a id="s-a66ebd922f"></a>`operation_contract · intent_schema · document · $defs · ReviewVariantIntent · properties · id · title` | `"Id"` |
| <a id="s-8d82170216"></a>`operation_contract · intent_schema · document · $defs · ReviewVariantIntent · properties · id · type` | `"string"` |
| <a id="s-5be592e298"></a>`operation_contract · intent_schema · document · $defs · ReviewVariantIntent · properties · portable_intent · additionalProperties · $ref` | `"#/$defs/JsonValue"` |
| <a id="s-1394d85ed9"></a>`operation_contract · intent_schema · document · $defs · ReviewVariantIntent · properties · portable_intent · title` | `"Portable Intent"` |
| <a id="s-0f54749acd"></a>`operation_contract · intent_schema · document · $defs · ReviewVariantIntent · properties · portable_intent · type` | `"object"` |
| <a id="s-026876d749"></a>`operation_contract · intent_schema · document · $defs · ReviewVariantIntent · required` | `["id","portable_intent"]` |
| <a id="s-c73099ca8a"></a>`operation_contract · intent_schema · document · $defs · ReviewVariantIntent · title` | `"ReviewVariantIntent"` |
| <a id="s-a964f28365"></a>`operation_contract · intent_schema · document · $defs · ReviewVariantIntent · type` | `"object"` |
| <a id="s-c032c1f5b4"></a>`operation_contract · intent_schema · document · $schema` | `"https://json-schema.org/draft/2020-12/schema"` |
| <a id="s-c2fa9e85f4"></a>`operation_contract · intent_schema · document · additionalProperties` | `false` |
| <a id="s-a2dae6e574"></a>`operation_contract · intent_schema · document · properties · sample_plan · $ref` | `"#/$defs/ReviewSamplePlan"` |
| <a id="s-1e3d7543b0"></a>`operation_contract · intent_schema · document · properties · variant · $ref` | `"#/$defs/ReviewVariantIntent"` |
| <a id="s-00201b84ac"></a>`operation_contract · intent_schema · document · required` | `["sample_plan","variant"]` |
| <a id="s-f7d34725b7"></a>`operation_contract · intent_schema · document · title` | `"ReviewMaterializeIntent"` |
| <a id="s-97321b0c11"></a>`operation_contract · intent_schema · document · type` | `"object"` |
| <a id="s-5481776bd9"></a>`operation_contract · intent_schema · format_policy` | `"annotation-only"` |
| <a id="s-3104393ed2"></a>`operation_contract · intent_schema · id` | `"stove0.review.materialize-intent/v1"` |
| <a id="s-0416e72a7f"></a>`operation_contract · intent_schema · profile_sha256` | `"eb997cd9a6f54adf0036ea548eca97df7ad335a8618d1af0c7ef65c190a80eaf"` |
| <a id="s-49507f8b2a"></a>`operation_contract · intent_semantics · conformance_vectors_sha256` | `"87c848c1e820178e863cf589b987cb392e4f3f85be1881e5dc3ce7e6de7f51ac"` |
| <a id="s-945baecccf"></a>`operation_contract · intent_semantics · id` | `"stove0.review.materialize-intent-semantics/v1"` |
| <a id="s-bf9b2a1d63"></a>`operation_contract · intent_semantics · profile_sha256` | `"0af5db45b2087ad40168ff5a95da2c41657e42c83b0c055c976181ecb773c373"` |
| <a id="s-5aa72332d2"></a>`operation_contract · intent_semantics · rules` | `["stove0.review.sample-plan.exact-declared-shape/v1","stove0.review.sample-plan.identity-verification/v1"]` |
| <a id="s-fa15e52c7d"></a>`operation_contract · outputs · item 1 · derived_from_roles` | `["stove0.review.source/v1"]` |
| <a id="s-58ef717c07"></a>`operation_contract · outputs · item 1 · maximum` | `null` |
| <a id="s-ba8a122ade"></a>`operation_contract · outputs · item 1 · minimum` | `0` |
| <a id="s-845fddedf8"></a>`operation_contract · outputs · item 1 · role` | `"stove0.review.audio/v1"` |
| <a id="s-5a43cb8d00"></a>`operation_contract · outputs · item 2 · derived_from_roles` | `["stove0.review.source/v1"]` |
| <a id="s-2ad1d5e687"></a>`operation_contract · outputs · item 2 · maximum` | `1` |
| <a id="s-c1dc6afbde"></a>`operation_contract · outputs · item 2 · minimum` | `1` |
| <a id="s-4748220645"></a>`operation_contract · outputs · item 2 · role` | `"stove0.review.index/v1"` |
| <a id="s-5be367c27d"></a>`operation_contract · outputs · item 3 · derived_from_roles` | `["stove0.review.source/v1"]` |
| <a id="s-d3139d32ee"></a>`operation_contract · outputs · item 3 · maximum` | `null` |
| <a id="s-fdcc08aa81"></a>`operation_contract · outputs · item 3 · minimum` | `0` |
| <a id="s-41fdc21fee"></a>`operation_contract · outputs · item 3 · role` | `"stove0.review.video/v1"` |
| <a id="s-8dab949f3d"></a>`operation_contract · result_kind` | `"collection"` |
| <a id="s-eda501ab60"></a>`operation_contract · source_collection_retirement_permitted` | `false` |
| <a id="s-6c1ee01e42"></a>`source_collection_retirement_permitted` | `false` |
| <a id="s-fbb8421b00"></a>`status` | `"conformant"` |

## Governing policies

- <a id="pa-3ba2398da5"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:review0-planner](../../../evidence/sources/authorities.md#src-cda082d53a) — [some-implementations/stove0/review0/planning/src/review0\_planner/conformance.py::&lt;module&gt;](../../../../../../some-implementations/stove0/review0/planning/src/review0_planner/conformance.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/review0-planner/allow_abbrev`
- `/external_contract/cli/review0-planner/name`
- `/external_contract/cli/review0-planner/parameters`
- `/external_contract/cli/review0-planner/result_contract`
- `/external_contract/cli/review0-planner/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/review0-planner/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/review0-planner/name`

<!-- exact-contract-value: 35ef4769d4b1dbd0273103f9955a66021f7beb31d7eb00dc7fe633a14c44dba3 -->

```json
"review0-planner"
```

### `/external_contract/cli/review0-planner/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/review0-planner/result_contract`

<!-- exact-contract-value: 5891280cf21ad8bd4a9829cf16bb6abe6a6e417ff2f6bd377ab4500a655dc08e -->

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
  "identity": "review0-planner-cli-result/root/v1",
  "profile_id": "review0-planner-cli/v1",
  "structured_output": "always-json",
  "success": [
    {
      "exit_status": 0,
      "id": "reported",
      "selected_by": {
        "kind": "contract-report-completed"
      },
      "stderr": {
        "all": "empty"
      },
      "stdout": {
        "json": {
          "document": {
            "format": "review0-contract-report/v1",
            "observer_contract": {
              "contract_sha256": "406dee0d01ed49e29d6b7738a89607d815a256473797711952628314ffe5e79a",
              "facts_schema": {
                "dialect": "https://json-schema.org/draft/2020-12/schema",
                "document": {
                  "$schema": "https://json-schema.org/draft/2020-12/schema",
                  "additionalProperties": false,
                  "properties": {
                    "artifacts": {
                      "items": {
                        "additionalProperties": false,
                        "properties": {
                          "artifact_id": {
                            "minLength": 1,
                            "type": "string"
                          },
                          "duration_ms": {
                            "minimum": 1,
                            "type": "integer"
                          },
                          "sampleable_ranges": {
                            "items": {
                              "additionalProperties": false,
                              "properties": {
                                "duration_ms": {
                                  "minimum": 1,
                                  "type": "integer"
                                },
                                "start_ms": {
                                  "minimum": 0,
                                  "type": "integer"
                                }
                              },
                              "required": [
                                "start_ms",
                                "duration_ms"
                              ],
                              "type": "object"
                            },
                            "minItems": 1,
                            "type": "array"
                          }
                        },
                        "required": [
                          "artifact_id",
                          "duration_ms",
                          "sampleable_ranges"
                        ],
                        "type": "object"
                      },
                      "minItems": 1,
                      "type": "array"
                    }
                  },
                  "required": [
                    "artifacts"
                  ],
                  "type": "object"
                },
                "format_policy": "annotation-only",
                "id": "stove0.review.media-sampling-facts/v1",
                "profile_sha256": "730e1fcd4a831bad9707814fe65fb59861359795bcf336ed65f72670c725f71a"
              },
              "facts_semantics": {
                "conformance_vectors_sha256": "6f6c2bc816116299fd5ad0dc3a8e1221c0058df224062677895ad2e06325f7e8",
                "id": "stove0.review.media-sampling-facts-semantics/v1",
                "profile_sha256": "ba56ae95653ee6d82e0505ca5b8794335f6e320edfe8395918a448c5861363da",
                "rules": [
                  "stove0.review.media-sampling-facts.complete-request-subjects/v1",
                  "stove0.review.media-sampling-facts.nonoverlapping-bounded-ranges/v1"
                ]
              },
              "id": "stove0.review.media-sampling/v1",
              "maximum_result_bytes": 262144,
              "options_schema": {
                "dialect": "https://json-schema.org/draft/2020-12/schema",
                "document": {
                  "$schema": "https://json-schema.org/draft/2020-12/schema",
                  "additionalProperties": false,
                  "properties": {},
                  "type": "object"
                },
                "format_policy": "annotation-only",
                "id": "stove0.review.media-sampling-options/v1",
                "profile_sha256": "9c523e70f370789ff20984ca7a638b47ada230c1d751869ce8dfc162db15b77a"
              }
            },
            "operation_contract": {
              "contract_sha256": "7d37787e93f61be7907eec95987cec02ab108d811c34b32c59a47ecd406c5efb",
              "effect_receipt_schema": null,
              "id": "stove0.review.materialize/v1",
              "inputs": [
                {
                  "allowed_dispositions": [
                    "preserved",
                    "transformed"
                  ],
                  "maximum": null,
                  "minimum": 1,
                  "role": "stove0.review.source/v1"
                }
              ],
              "intent_schema": {
                "dialect": "https://json-schema.org/draft/2020-12/schema",
                "document": {
                  "$defs": {
                    "JsonValue": {},
                    "ReviewSamplePlan": {
                      "additionalProperties": false,
                      "properties": {
                        "format": {
                          "const": "review0-sample-plan/v1",
                          "default": "review0-sample-plan/v1",
                          "title": "Format",
                          "type": "string"
                        },
                        "sample_plan_sha256": {
                          "pattern": "^[0-9a-f]{64}$",
                          "title": "Sample Plan Sha256",
                          "type": "string"
                        },
                        "samples_per_artifact": {
                          "minimum": 1,
                          "title": "Samples Per Artifact",
                          "type": "integer"
                        },
                        "selection_method": {
                          "const": "evenly-spaced/v1",
                          "default": "evenly-spaced/v1",
                          "title": "Selection Method",
                          "type": "string"
                        },
                        "window_duration_ms": {
                          "minimum": 1,
                          "title": "Window Duration Ms",
                          "type": "integer"
                        },
                        "windows": {
                          "items": {
                            "$ref": "#/$defs/ReviewSampleWindow"
                          },
                          "minItems": 1,
                          "title": "Windows",
                          "type": "array"
                        }
                      },
                      "required": [
                        "samples_per_artifact",
                        "window_duration_ms",
                        "windows",
                        "sample_plan_sha256"
                      ],
                      "title": "ReviewSamplePlan",
                      "type": "object"
                    },
                    "ReviewSampleWindow": {
                      "additionalProperties": false,
                      "properties": {
                        "artifact_id": {
                          "maxLength": 160,
                          "minLength": 1,
                          "title": "Artifact Id",
                          "type": "string"
                        },
                        "duration_ms": {
                          "minimum": 1,
                          "title": "Duration Ms",
                          "type": "integer"
                        },
                        "start_ms": {
                          "minimum": 0,
                          "title": "Start Ms",
                          "type": "integer"
                        }
                      },
                      "required": [
                        "artifact_id",
                        "start_ms",
                        "duration_ms"
                      ],
                      "title": "ReviewSampleWindow",
                      "type": "object"
                    },
                    "ReviewVariantIntent": {
                      "additionalProperties": false,
                      "properties": {
                        "id": {
                          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._-]{0,158}[a-z0-9])?$",
                          "title": "Id",
                          "type": "string"
                        },
                        "portable_intent": {
                          "additionalProperties": {
                            "$ref": "#/$defs/JsonValue"
                          },
                          "title": "Portable Intent",
                          "type": "object"
                        }
                      },
                      "required": [
                        "id",
                        "portable_intent"
                      ],
                      "title": "ReviewVariantIntent",
                      "type": "object"
                    }
                  },
                  "$schema": "https://json-schema.org/draft/2020-12/schema",
                  "additionalProperties": false,
                  "properties": {
                    "sample_plan": {
                      "$ref": "#/$defs/ReviewSamplePlan"
                    },
                    "variant": {
                      "$ref": "#/$defs/ReviewVariantIntent"
                    }
                  },
                  "required": [
                    "sample_plan",
                    "variant"
                  ],
                  "title": "ReviewMaterializeIntent",
                  "type": "object"
                },
                "format_policy": "annotation-only",
                "id": "stove0.review.materialize-intent/v1",
                "profile_sha256": "eb997cd9a6f54adf0036ea548eca97df7ad335a8618d1af0c7ef65c190a80eaf"
              },
              "intent_semantics": {
                "conformance_vectors_sha256": "87c848c1e820178e863cf589b987cb392e4f3f85be1881e5dc3ce7e6de7f51ac",
                "id": "stove0.review.materialize-intent-semantics/v1",
                "profile_sha256": "0af5db45b2087ad40168ff5a95da2c41657e42c83b0c055c976181ecb773c373",
                "rules": [
                  "stove0.review.sample-plan.exact-declared-shape/v1",
                  "stove0.review.sample-plan.identity-verification/v1"
                ]
              },
              "outputs": [
                {
                  "derived_from_roles": [
                    "stove0.review.source/v1"
                  ],
                  "maximum": null,
                  "minimum": 0,
                  "role": "stove0.review.audio/v1"
                },
                {
                  "derived_from_roles": [
                    "stove0.review.source/v1"
                  ],
                  "maximum": 1,
                  "minimum": 1,
                  "role": "stove0.review.index/v1"
                },
                {
                  "derived_from_roles": [
                    "stove0.review.source/v1"
                  ],
                  "maximum": null,
                  "minimum": 0,
                  "role": "stove0.review.video/v1"
                }
              ],
              "result_kind": "collection",
              "source_collection_retirement_permitted": false
            },
            "source_collection_retirement_permitted": false,
            "status": "conformant"
          },
          "identity": "review0-contract-report/v1",
          "kind": "cli-local-exact-json"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/review0-planner/terminating_controls`

<!-- exact-contract-value: 46c96c22d2ed8a51da57bba3ac0f2269bb35f98c5fd6dc3e3ba67e60f772ad72 -->

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
  }
]
```

</details>
