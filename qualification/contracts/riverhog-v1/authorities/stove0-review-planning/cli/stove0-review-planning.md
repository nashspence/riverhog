# stove0-review-planning

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-review-planning:stove0-review-planning:d83ce63aee -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-planning](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-dae9ec8755"></a>Parser name: `stove0-review-planning`

| Field | Value |
|---|---|
| <a id="s-0de208a21c"></a>`parameters` | `[]` |
- <a id="s-86b1c7e7d9"></a>Unique long-option abbreviations: accepted.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-0d3817d373"></a>`help` | <a id="s-e4ec60d935"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-795490d3e3"></a>`0` | <a id="s-fd97008e96"></a>`"noncontractual-framework-help"` | <a id="s-1f058ba026"></a>`"empty"` |

### Result and failure contract

- <a id="s-b591048dbb"></a>Result identity: `stove0-review-planning-cli-result/root/v1`
- <a id="s-25b284cc0e"></a>Profile: `stove0-review-planning-cli/v1`
- <a id="s-c546bb1483"></a>Structured output: `always-json`
- <a id="s-b236ff411e"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-0e1a4ad46c"></a>`reported` | <a id="s-5464cc7104"></a>`{"kind":"contract-report-completed"}` | <a id="s-030349a6ea"></a>`0` | <a id="s-eeb2270368"></a>json: [stove0-review-contract-report/v1](#s-ae86968a87) | <a id="s-412d1eb2d2"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-5fb3bb619b"></a>`usage` | <a id="s-8e15c1c4d7"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-6549be7b49"></a>`2` | <a id="s-2e941bb0af"></a>all: `"empty"` | <a id="s-69d9fcf861"></a>all: `"noncontractual-usage-diagnostic"` |

### Local structured outputs


#### <a id="s-ae86968a87"></a>`stove0-review-contract-report/v1`

Applies to: reported · stdout (json).

<a id="s-0af7606fc9"></a>

| Field | Value |
|---|---|
| <a id="s-b884c01d55"></a>`format` | `"stove0-review-contract-report/v1"` |
| <a id="s-326bc1f814"></a>`observer_contract · contract_sha256` | `"0e0cdebd908511848dc7953a045587ac141c689bbd64c831403f531bb70371c6"` |
| <a id="s-9cf66b7af3"></a>`observer_contract · facts_schema · dialect` | `"https://json-schema.org/draft/2020-12/schema"` |
| <a id="s-16c24f5c2c"></a>`observer_contract · facts_schema · document · $schema` | `"https://json-schema.org/draft/2020-12/schema"` |
| <a id="s-82b0a95fe3"></a>`observer_contract · facts_schema · document · additionalProperties` | `false` |
| <a id="s-c9c9dfa961"></a>`observer_contract · facts_schema · document · properties · artifacts · items · additionalProperties` | `false` |
| <a id="s-093d35852f"></a>`observer_contract · facts_schema · document · properties · artifacts · items · properties · artifact_id · minLength` | `1` |
| <a id="s-13aeed9d95"></a>`observer_contract · facts_schema · document · properties · artifacts · items · properties · artifact_id · type` | `"string"` |
| <a id="s-2a841d20a6"></a>`observer_contract · facts_schema · document · properties · artifacts · items · properties · duration_ms · minimum` | `1` |
| <a id="s-7b04291baa"></a>`observer_contract · facts_schema · document · properties · artifacts · items · properties · duration_ms · type` | `"integer"` |
| <a id="s-fbcad2e2bf"></a>`observer_contract · facts_schema · document · properties · artifacts · items · properties · sampleable_ranges · items · additionalProperties` | `false` |
| <a id="s-861de4d5ff"></a>`observer_contract · facts_schema · document · properties · artifacts · items · properties · sampleable_ranges · items · properties · duration_ms · minimum` | `1` |
| <a id="s-e6a2ceada8"></a>`observer_contract · facts_schema · document · properties · artifacts · items · properties · sampleable_ranges · items · properties · duration_ms · type` | `"integer"` |
| <a id="s-137b99118f"></a>`observer_contract · facts_schema · document · properties · artifacts · items · properties · sampleable_ranges · items · properties · start_ms · minimum` | `0` |
| <a id="s-be287ec276"></a>`observer_contract · facts_schema · document · properties · artifacts · items · properties · sampleable_ranges · items · properties · start_ms · type` | `"integer"` |
| <a id="s-b371e8155f"></a>`observer_contract · facts_schema · document · properties · artifacts · items · properties · sampleable_ranges · items · required` | `["start_ms","duration_ms"]` |
| <a id="s-dcd9566340"></a>`observer_contract · facts_schema · document · properties · artifacts · items · properties · sampleable_ranges · items · type` | `"object"` |
| <a id="s-b5a8b97a40"></a>`observer_contract · facts_schema · document · properties · artifacts · items · properties · sampleable_ranges · minItems` | `1` |
| <a id="s-65cfd3360b"></a>`observer_contract · facts_schema · document · properties · artifacts · items · properties · sampleable_ranges · type` | `"array"` |
| <a id="s-c781bba846"></a>`observer_contract · facts_schema · document · properties · artifacts · items · required` | `["artifact_id","duration_ms","sampleable_ranges"]` |
| <a id="s-41d5e857f0"></a>`observer_contract · facts_schema · document · properties · artifacts · items · type` | `"object"` |
| <a id="s-2cc2ba24b3"></a>`observer_contract · facts_schema · document · properties · artifacts · minItems` | `1` |
| <a id="s-198cb9be1c"></a>`observer_contract · facts_schema · document · properties · artifacts · type` | `"array"` |
| <a id="s-7f18ae6b4c"></a>`observer_contract · facts_schema · document · required` | `["artifacts"]` |
| <a id="s-97d11f4512"></a>`observer_contract · facts_schema · document · type` | `"object"` |
| <a id="s-3238c8ac66"></a>`observer_contract · facts_schema · format_policy` | `"annotation-only"` |
| <a id="s-41199aa23f"></a>`observer_contract · facts_schema · id` | `"stove0.review.media-sampling-facts/v1"` |
| <a id="s-2db503e01c"></a>`observer_contract · facts_schema · profile_sha256` | `"730e1fcd4a831bad9707814fe65fb59861359795bcf336ed65f72670c725f71a"` |
| <a id="s-0aa83751b3"></a>`observer_contract · facts_semantics · conformance_vectors_sha256` | `"7fa8220ce785a840b854d24a70b9c4b6da99c853ab0b6eba4100d89ae1fb23f8"` |
| <a id="s-8ad269b184"></a>`observer_contract · facts_semantics · id` | `"stove0.review.media-sampling-facts-semantics/v1"` |
| <a id="s-eaeaba3b35"></a>`observer_contract · facts_semantics · profile_sha256` | `"6f91eaa38e4152a6db4d9d7f6c157295ac7d30fc412fef92ebcd5120de8fce7e"` |
| <a id="s-b57e4650eb"></a>`observer_contract · facts_semantics · rules` | `["stove0.review.media-sampling-facts.complete-request-subjects/v1","stove0.review.media-sampling-facts.nonoverlapping-bounded-ranges/v1"]` |
| <a id="s-1808a27b84"></a>`observer_contract · id` | `"stove0.review.media-sampling/v1"` |
| <a id="s-4e8642a6b6"></a>`observer_contract · maximum_result_bytes` | `262144` |
| <a id="s-c87f86bd5f"></a>`observer_contract · options_schema · dialect` | `"https://json-schema.org/draft/2020-12/schema"` |
| <a id="s-3c11b590d0"></a>`observer_contract · options_schema · document · $schema` | `"https://json-schema.org/draft/2020-12/schema"` |
| <a id="s-7dbd8ee668"></a>`observer_contract · options_schema · document · additionalProperties` | `false` |
| <a id="s-1e8dacbc10"></a>`observer_contract · options_schema · document · properties` | `{}` |
| <a id="s-45269c26e0"></a>`observer_contract · options_schema · document · type` | `"object"` |
| <a id="s-6f2287927a"></a>`observer_contract · options_schema · format_policy` | `"annotation-only"` |
| <a id="s-5fa0e92c9d"></a>`observer_contract · options_schema · id` | `"stove0.review.media-sampling-options/v1"` |
| <a id="s-a804dcdb36"></a>`observer_contract · options_schema · profile_sha256` | `"9c523e70f370789ff20984ca7a638b47ada230c1d751869ce8dfc162db15b77a"` |
| <a id="s-af8c1fdb27"></a>`operation_contract · contract_sha256` | `"47f85e10c52a329c4a30d23a0bbd926d6dbd7c5a004d85c01e99e3bc606ffc5a"` |
| <a id="s-90e5b5db3c"></a>`operation_contract · effect_receipt_schema` | `null` |
| <a id="s-a4e039afd8"></a>`operation_contract · id` | `"stove0.review.materialize/v1"` |
| <a id="s-ef1f5737f1"></a>`operation_contract · inputs · item 1 · allowed_dispositions` | `["preserved","transformed"]` |
| <a id="s-8193947069"></a>`operation_contract · inputs · item 1 · maximum` | `null` |
| <a id="s-3e2595a813"></a>`operation_contract · inputs · item 1 · minimum` | `1` |
| <a id="s-82b9a48f06"></a>`operation_contract · inputs · item 1 · role` | `"stove0.review.source/v1"` |
| <a id="s-b47cb38b21"></a>`operation_contract · intent_schema · dialect` | `"https://json-schema.org/draft/2020-12/schema"` |
| <a id="s-89fb3aaaae"></a>`operation_contract · intent_schema · document · $defs · JsonValue` | `{}` |
| <a id="s-b76402410d"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · additionalProperties` | `false` |
| <a id="s-65f9e1e7f3"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · properties · format · const` | `"stove0-review-sample-plan/v1"` |
| <a id="s-f5fec22674"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · properties · format · default` | `"stove0-review-sample-plan/v1"` |
| <a id="s-f0484eeda3"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · properties · format · title` | `"Format"` |
| <a id="s-fd1795362a"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · properties · format · type` | `"string"` |
| <a id="s-02ad865bff"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · properties · sample_plan_sha256 · pattern` | `"^[0-9a-f]{64}$"` |
| <a id="s-53a914d87d"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · properties · sample_plan_sha256 · title` | `"Sample Plan Sha256"` |
| <a id="s-f3f598c065"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · properties · sample_plan_sha256 · type` | `"string"` |
| <a id="s-03e6f246e8"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · properties · samples_per_artifact · minimum` | `1` |
| <a id="s-c00012f1d5"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · properties · samples_per_artifact · title` | `"Samples Per Artifact"` |
| <a id="s-b8501de591"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · properties · samples_per_artifact · type` | `"integer"` |
| <a id="s-7d5118873b"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · properties · selection_method · const` | `"evenly-spaced/v1"` |
| <a id="s-cd1e4113a9"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · properties · selection_method · default` | `"evenly-spaced/v1"` |
| <a id="s-4ea9c28a27"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · properties · selection_method · title` | `"Selection Method"` |
| <a id="s-f1efb6ba2c"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · properties · selection_method · type` | `"string"` |
| <a id="s-356a9b59ee"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · properties · window_duration_ms · minimum` | `1` |
| <a id="s-b02bdd891e"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · properties · window_duration_ms · title` | `"Window Duration Ms"` |
| <a id="s-a571cd4f26"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · properties · window_duration_ms · type` | `"integer"` |
| <a id="s-4d1751a4a9"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · properties · windows · items · $ref` | `"#/$defs/ReviewSampleWindow"` |
| <a id="s-20ced1adda"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · properties · windows · minItems` | `1` |
| <a id="s-4037f2483e"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · properties · windows · title` | `"Windows"` |
| <a id="s-ea190008f5"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · properties · windows · type` | `"array"` |
| <a id="s-f3ef7ab028"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · required` | `["samples_per_artifact","window_duration_ms","windows","sample_plan_sha256"]` |
| <a id="s-8b9574e333"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · title` | `"ReviewSamplePlan"` |
| <a id="s-d65c4c7fc5"></a>`operation_contract · intent_schema · document · $defs · ReviewSamplePlan · type` | `"object"` |
| <a id="s-975bf8db08"></a>`operation_contract · intent_schema · document · $defs · ReviewSampleWindow · additionalProperties` | `false` |
| <a id="s-7212c1cc0a"></a>`operation_contract · intent_schema · document · $defs · ReviewSampleWindow · properties · artifact_id · maxLength` | `160` |
| <a id="s-e32217a1f1"></a>`operation_contract · intent_schema · document · $defs · ReviewSampleWindow · properties · artifact_id · minLength` | `1` |
| <a id="s-c7fc16a2eb"></a>`operation_contract · intent_schema · document · $defs · ReviewSampleWindow · properties · artifact_id · title` | `"Artifact Id"` |
| <a id="s-641a5a56fb"></a>`operation_contract · intent_schema · document · $defs · ReviewSampleWindow · properties · artifact_id · type` | `"string"` |
| <a id="s-210ec07629"></a>`operation_contract · intent_schema · document · $defs · ReviewSampleWindow · properties · duration_ms · minimum` | `1` |
| <a id="s-db6152b96f"></a>`operation_contract · intent_schema · document · $defs · ReviewSampleWindow · properties · duration_ms · title` | `"Duration Ms"` |
| <a id="s-2257b04ee7"></a>`operation_contract · intent_schema · document · $defs · ReviewSampleWindow · properties · duration_ms · type` | `"integer"` |
| <a id="s-0934d2d5d3"></a>`operation_contract · intent_schema · document · $defs · ReviewSampleWindow · properties · start_ms · minimum` | `0` |
| <a id="s-a8b2118d66"></a>`operation_contract · intent_schema · document · $defs · ReviewSampleWindow · properties · start_ms · title` | `"Start Ms"` |
| <a id="s-3c7c10eb88"></a>`operation_contract · intent_schema · document · $defs · ReviewSampleWindow · properties · start_ms · type` | `"integer"` |
| <a id="s-c07cc47822"></a>`operation_contract · intent_schema · document · $defs · ReviewSampleWindow · required` | `["artifact_id","start_ms","duration_ms"]` |
| <a id="s-cdfbeb0bfb"></a>`operation_contract · intent_schema · document · $defs · ReviewSampleWindow · title` | `"ReviewSampleWindow"` |
| <a id="s-cee86815c7"></a>`operation_contract · intent_schema · document · $defs · ReviewSampleWindow · type` | `"object"` |
| <a id="s-0d403eac30"></a>`operation_contract · intent_schema · document · $defs · ReviewVariantIntent · additionalProperties` | `false` |
| <a id="s-8ca8ff3a8e"></a>`operation_contract · intent_schema · document · $defs · ReviewVariantIntent · properties · id · pattern` | `"^[a-z0-9]&#40;?:[a-z0-9._-]{0,158}[a-z0-9])?$"` |
| <a id="s-b34d53a20c"></a>`operation_contract · intent_schema · document · $defs · ReviewVariantIntent · properties · id · title` | `"Id"` |
| <a id="s-a13a89e95d"></a>`operation_contract · intent_schema · document · $defs · ReviewVariantIntent · properties · id · type` | `"string"` |
| <a id="s-792a96019b"></a>`operation_contract · intent_schema · document · $defs · ReviewVariantIntent · properties · portable_intent · additionalProperties · $ref` | `"#/$defs/JsonValue"` |
| <a id="s-556c48944d"></a>`operation_contract · intent_schema · document · $defs · ReviewVariantIntent · properties · portable_intent · title` | `"Portable Intent"` |
| <a id="s-df2dfc04a6"></a>`operation_contract · intent_schema · document · $defs · ReviewVariantIntent · properties · portable_intent · type` | `"object"` |
| <a id="s-d4cc8410e5"></a>`operation_contract · intent_schema · document · $defs · ReviewVariantIntent · required` | `["id","portable_intent"]` |
| <a id="s-9597ce8cd0"></a>`operation_contract · intent_schema · document · $defs · ReviewVariantIntent · title` | `"ReviewVariantIntent"` |
| <a id="s-31e56a9f38"></a>`operation_contract · intent_schema · document · $defs · ReviewVariantIntent · type` | `"object"` |
| <a id="s-b869ea22c3"></a>`operation_contract · intent_schema · document · $schema` | `"https://json-schema.org/draft/2020-12/schema"` |
| <a id="s-adb8fc654f"></a>`operation_contract · intent_schema · document · additionalProperties` | `false` |
| <a id="s-5e91eda9a6"></a>`operation_contract · intent_schema · document · properties · sample_plan · $ref` | `"#/$defs/ReviewSamplePlan"` |
| <a id="s-dd773c6208"></a>`operation_contract · intent_schema · document · properties · variant · $ref` | `"#/$defs/ReviewVariantIntent"` |
| <a id="s-0fe9a00c20"></a>`operation_contract · intent_schema · document · required` | `["sample_plan","variant"]` |
| <a id="s-32166d8a5c"></a>`operation_contract · intent_schema · document · title` | `"ReviewMaterializeIntent"` |
| <a id="s-0cede4721a"></a>`operation_contract · intent_schema · document · type` | `"object"` |
| <a id="s-0643123018"></a>`operation_contract · intent_schema · format_policy` | `"annotation-only"` |
| <a id="s-909926582a"></a>`operation_contract · intent_schema · id` | `"stove0.review.materialize-intent/v1"` |
| <a id="s-607b9f13a5"></a>`operation_contract · intent_schema · profile_sha256` | `"dacf9977497657d10678176bb83ecf7783db08114b417390d577810d47ef2b30"` |
| <a id="s-b75a49ff65"></a>`operation_contract · intent_semantics · conformance_vectors_sha256` | `"eda75742bb220994e735aa6e00cb0f598dbac7de55e3cdd9576cbe4944789583"` |
| <a id="s-a3631ae26c"></a>`operation_contract · intent_semantics · id` | `"stove0.review.materialize-intent-semantics/v1"` |
| <a id="s-e0686c8a9d"></a>`operation_contract · intent_semantics · profile_sha256` | `"4625bc86e212e403da151d0c6dfe1f7a6cbf9832adea7161ffcec32656c1b79c"` |
| <a id="s-4165d3b14b"></a>`operation_contract · intent_semantics · rules` | `["stove0.review.sample-plan.exact-declared-shape/v1","stove0.review.sample-plan.identity-verification/v1"]` |
| <a id="s-2207b2d293"></a>`operation_contract · outputs · item 1 · derived_from_roles` | `["stove0.review.source/v1"]` |
| <a id="s-f4d25047df"></a>`operation_contract · outputs · item 1 · maximum` | `null` |
| <a id="s-8490d1f94f"></a>`operation_contract · outputs · item 1 · minimum` | `0` |
| <a id="s-54a00cce72"></a>`operation_contract · outputs · item 1 · role` | `"stove0.review.audio/v1"` |
| <a id="s-701085837a"></a>`operation_contract · outputs · item 2 · derived_from_roles` | `["stove0.review.source/v1"]` |
| <a id="s-acd01724df"></a>`operation_contract · outputs · item 2 · maximum` | `1` |
| <a id="s-6e3d69a8f1"></a>`operation_contract · outputs · item 2 · minimum` | `1` |
| <a id="s-a3764abea4"></a>`operation_contract · outputs · item 2 · role` | `"stove0.review.index/v1"` |
| <a id="s-21392d33cb"></a>`operation_contract · outputs · item 3 · derived_from_roles` | `["stove0.review.source/v1"]` |
| <a id="s-6bb26bd6dd"></a>`operation_contract · outputs · item 3 · maximum` | `null` |
| <a id="s-9280f10aaf"></a>`operation_contract · outputs · item 3 · minimum` | `0` |
| <a id="s-e6b028344a"></a>`operation_contract · outputs · item 3 · role` | `"stove0.review.video/v1"` |
| <a id="s-74b52e3d74"></a>`operation_contract · result_kind` | `"collection"` |
| <a id="s-4398bf8179"></a>`operation_contract · source_retirement_permitted` | `false` |
| <a id="s-9c3bec118d"></a>`source_retirement_permitted` | `false` |
| <a id="s-10e98a2504"></a>`status` | `"conformant"` |

## Governing policies

- <a id="pa-cfaa1d3c31"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:stove0-review-planning](../../../evidence/sources/authorities.md#src-ae789ab860) — [reference/stove0/targets/review/planning/src/stove0\_review\_planning/conformance.py::&lt;module&gt;](../../../../../../reference/stove0/targets/review/planning/src/stove0_review_planning/conformance.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/stove0-review-planning/allow_abbrev`
- `/external_contract/cli/stove0-review-planning/name`
- `/external_contract/cli/stove0-review-planning/parameters`
- `/external_contract/cli/stove0-review-planning/result_contract`
- `/external_contract/cli/stove0-review-planning/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0-review-planning/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0-review-planning/name`

<!-- exact-contract-value: 5d8ffc9dd6189ed095a3b442507a319ce446bbd6db2c2de9e1dea956495a0614 -->

```json
"stove0-review-planning"
```

### `/external_contract/cli/stove0-review-planning/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/stove0-review-planning/result_contract`

<!-- exact-contract-value: c8fb5d26723189cc16b7faef256cd58d312663dc855c01de3500e63466175e8a -->

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
  "identity": "stove0-review-planning-cli-result/root/v1",
  "profile_id": "stove0-review-planning-cli/v1",
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
            "format": "stove0-review-contract-report/v1",
            "observer_contract": {
              "contract_sha256": "0e0cdebd908511848dc7953a045587ac141c689bbd64c831403f531bb70371c6",
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
                "conformance_vectors_sha256": "7fa8220ce785a840b854d24a70b9c4b6da99c853ab0b6eba4100d89ae1fb23f8",
                "id": "stove0.review.media-sampling-facts-semantics/v1",
                "profile_sha256": "6f91eaa38e4152a6db4d9d7f6c157295ac7d30fc412fef92ebcd5120de8fce7e",
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
              "contract_sha256": "47f85e10c52a329c4a30d23a0bbd926d6dbd7c5a004d85c01e99e3bc606ffc5a",
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
                          "const": "stove0-review-sample-plan/v1",
                          "default": "stove0-review-sample-plan/v1",
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
                "profile_sha256": "dacf9977497657d10678176bb83ecf7783db08114b417390d577810d47ef2b30"
              },
              "intent_semantics": {
                "conformance_vectors_sha256": "eda75742bb220994e735aa6e00cb0f598dbac7de55e3cdd9576cbe4944789583",
                "id": "stove0.review.materialize-intent-semantics/v1",
                "profile_sha256": "4625bc86e212e403da151d0c6dfe1f7a6cbf9832adea7161ffcec32656c1b79c",
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
              "source_retirement_permitted": false
            },
            "source_retirement_permitted": false,
            "status": "conformant"
          },
          "identity": "stove0-review-contract-report/v1",
          "kind": "cli-local-exact-json"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/stove0-review-planning/terminating_controls`

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
