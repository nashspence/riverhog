# stove0-review-planning

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-review-planning:stove0-review-planning:d83ce63aee -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-planning](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-dae9ec8755"></a>Parser name: `stove0-review-planning`
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
| <a id="s-0e1a4ad46c"></a>`reported` | <a id="s-5464cc7104"></a>`{"kind":"contract-report-completed"}` | <a id="s-030349a6ea"></a>`0` | <a id="s-eeb2270368"></a>json: [stove0-review-contract-report/v1](#s-eeb2270368) | <a id="s-412d1eb2d2"></a>all: `empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-5fb3bb619b"></a>`usage` | <a id="s-8e15c1c4d7"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-6549be7b49"></a>`2` | <a id="s-2e941bb0af"></a>all: `empty` | <a id="s-69d9fcf861"></a>all: `noncontractual-usage-diagnostic` |

## Governing policies

- <a id="pa-cfaa1d3c31"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0-review-planning](../../../evidence/sources.md#src-ae789ab860) — `reference/stove0/targets/review/planning/src/stove0_review_planning/conformance.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0-review-planning/allow_abbrev`
- `/external_contract/cli/stove0-review-planning/name`
- `/external_contract/cli/stove0-review-planning/parameters`
- `/external_contract/cli/stove0-review-planning/result_contract`
- `/external_contract/cli/stove0-review-planning/terminating_controls`

### Exact owned JSON

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

<!-- exact-contract-value: 17a2c7ad2f81bc115350c21007fb983626ee3bea87984e11cab31c61d9b7436c -->

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
              "contract_sha256": "beb0c37e896b0b2e3a8818f667ff59628614a461b06dc9f83560db434525a5be",
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
                "sha256": "730e1fcd4a831bad9707814fe65fb59861359795bcf336ed65f72670c725f71a"
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
                "sha256": "9c523e70f370789ff20984ca7a638b47ada230c1d751869ce8dfc162db15b77a"
              }
            },
            "operation_contract": {
              "contract_sha256": "5517370bb1016dfadd236f0f06300999e18de90537bbdad46b87f274958ab06d",
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
                "sha256": "dacf9977497657d10678176bb83ecf7783db08114b417390d577810d47ef2b30"
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
