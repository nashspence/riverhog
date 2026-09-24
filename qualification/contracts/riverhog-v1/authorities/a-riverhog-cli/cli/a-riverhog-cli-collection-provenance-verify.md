# a-riverhog-cli collection provenance verify

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-collection-provenance-verify:ed6e0f4724 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-c713f301b5"></a>Parser name: `verify`
- <a id="s-702691998f"></a>Extra arguments at this parser: rejected.
- <a id="s-10a51cc3d0"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-c8af64e004"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-211383a403"></a>`collection_id`<br>`collection_id` | required positional; 1 value | integer | not recorded<br>Env: `null` |
| <a id="s-3b0f9e0de0"></a>`wait`<br>`--wait`, alternate: `--no-wait` | optional flag; 0 values | boolean | `true`<br>Env: `null` |
| <a id="s-3715fc7456"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-cad8a1173f"></a>`help` | <a id="s-5ee038a174"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-1340edc17a"></a>`0` | <a id="s-70c7625cc3"></a>`"noncontractual-framework-help"` | <a id="s-2eaf26aafa"></a>`"empty"` |

### Result and failure contract

- <a id="s-fbeae367f5"></a>Result identity: `a-riverhog-cli-result/collection/provenance/verify/v1`
- <a id="s-5d739ceb4d"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-f08b20f358"></a>Structured output: `optional-json`
- <a id="s-9e8e9b0182"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-3f4d09d8a0"></a>`completed` | <a id="s-4df1dd2a96"></a>`{"kind":"command-completed"}` | <a id="s-cd2a8f3d54"></a>`0` | <a id="s-ea04c32fbf"></a>human: `"noncontractual-presentation-of-command-result"`; json: [OpenAPI riverhog.CollectionProvenanceVerificationJobOut](../../riverhog/http-schemas/schemas-collectionprovenanceverificationjobout.md) | <a id="s-8e3c94075a"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-f2751890ba"></a>`usage` | <a id="s-b0946250a2"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-3b2b9f4154"></a>`2` | <a id="s-43e4ab1168"></a>all: `"empty"` | <a id="s-4c4ec39814"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-0c8c0eddca"></a>`operational` | <a id="s-5fbfdafec8"></a>`{"kind":"application-error"}` | <a id="s-3e3efa3aba"></a>`1` | <a id="s-ece3707b5e"></a>human: `"empty"`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-a40a7516b4"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-211383a403) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-3715fc7456) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter --wait](#s-3b0f9e0de0) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |

## Maintained corroboration

### Related interface records

- [GET /v1/collections/{collection_id}/provenance/verification](../../riverhog/http-operations/get-v1-collections-collection-id-provenance-verification.md)
- [POST /v1/collections/{collection_id}/provenance/verification](../../riverhog/http-operations/post-v1-collections-collection-id-provenance-verification.md)
- [riverhog_client.ApiClient.get_collection_provenance_verification](../../riverhog-client/python/riverhog-client-apiclient-get-collection-provenance-verification.md)
- [riverhog_client.ApiClient.request_collection_provenance_verification](../../riverhog-client/python/riverhog-client-apiclient-request-collection-provenance-verification.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-5fb5c625b1"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-ff9b796979"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::provenance\_verify\_cmd](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py#L2721)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/verify/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/verify/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/verify/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/verify/name`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/verify/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/verify/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/verify/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/verify/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/verify/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/verify/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/verify/name`

<!-- exact-contract-value: 898c74c2eed0452b1e51e567f237c37f1caa1e52f747466d56e76e15d07dc331 -->

```json
"verify"
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/verify/parameters`

<!-- exact-contract-value: 70fe05f53caa8db16a24d77f71fdd8a4a8136f72f750deca8818bcc124574e64 -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "collection_id",
    "nargs": 1,
    "options": [
      "collection_id"
    ],
    "required": true,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.IntParamType",
      "name": "integer"
    }
  },
  {
    "count": false,
    "default": true,
    "envvar": null,
    "is_flag": true,
    "kind": "TyperOption",
    "multiple": false,
    "name": "wait",
    "nargs": 1,
    "options": [
      "--wait"
    ],
    "required": false,
    "secondary_options": [
      "--no-wait"
    ],
    "type": {
      "class": "typer._click.types.BoolParamType",
      "name": "boolean"
    }
  },
  {
    "count": false,
    "default": false,
    "envvar": null,
    "is_flag": true,
    "kind": "TyperOption",
    "multiple": false,
    "name": "json_mode",
    "nargs": 1,
    "options": [
      "--json"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.BoolParamType",
      "name": "boolean"
    }
  }
]
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/verify/result_contract`

<!-- exact-contract-value: ba278011e8ece020f1f8fcb65a96fa36391005553fa68293a61cbc5fdcc58523 -->

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
    },
    {
      "exit_status": 1,
      "id": "operational",
      "selected_by": {
        "kind": "application-error"
      },
      "stderr": {
        "human": "noncontractual-diagnostic",
        "json": "empty"
      },
      "stdout": {
        "human": "empty",
        "json": {
          "identity": "http-api-contracts.ErrorResponse",
          "kind": "python-model",
          "schema": {
            "$defs": {
              "ErrorBody": {
                "additionalProperties": false,
                "properties": {
                  "code": {
                    "minLength": 1,
                    "title": "Code",
                    "type": "string"
                  },
                  "details": {
                    "anyOf": [
                      {
                        "additionalProperties": true,
                        "type": "object"
                      },
                      {
                        "type": "null"
                      }
                    ],
                    "default": null,
                    "title": "Details"
                  },
                  "message": {
                    "minLength": 1,
                    "title": "Message",
                    "type": "string"
                  }
                },
                "required": [
                  "code",
                  "message"
                ],
                "title": "ErrorBody",
                "type": "object"
              }
            },
            "additionalProperties": false,
            "properties": {
              "error": {
                "$ref": "#/$defs/ErrorBody"
              }
            },
            "required": [
              "error"
            ],
            "title": "ErrorResponse",
            "type": "object"
          }
        }
      }
    }
  ],
  "human_json_relationship": "same-semantic-result",
  "identity": "a-riverhog-cli-result/collection/provenance/verify/v1",
  "profile_id": "a-riverhog-cli-human-json/v1",
  "structured_output": "optional-json",
  "success": [
    {
      "exit_status": 0,
      "id": "completed",
      "selected_by": {
        "kind": "command-completed"
      },
      "stderr": {
        "all": "empty"
      },
      "stdout": {
        "human": "noncontractual-presentation-of-command-result",
        "json": {
          "application": "riverhog",
          "definition": {
            "additionalProperties": false,
            "properties": {
              "attempts": {
                "minimum": 0,
                "title": "Attempts",
                "type": "integer"
              },
              "collection_id": {
                "$ref": "#/components/schemas/CollectionId"
              },
              "failure": {
                "anyOf": [
                  {
                    "type": "string"
                  },
                  {
                    "type": "null"
                  }
                ],
                "title": "Failure"
              },
              "finished_at": {
                "anyOf": [
                  {
                    "maxLength": 30,
                    "minLength": 30,
                    "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
                    "type": "string"
                  },
                  {
                    "type": "null"
                  }
                ],
                "title": "Finished At"
              },
              "requested_at": {
                "maxLength": 30,
                "minLength": 30,
                "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
                "title": "Requested At",
                "type": "string"
              },
              "result": {
                "anyOf": [
                  {
                    "$ref": "#/components/schemas/CollectionProvenanceVerificationOut"
                  },
                  {
                    "type": "null"
                  }
                ]
              },
              "started_at": {
                "anyOf": [
                  {
                    "maxLength": 30,
                    "minLength": 30,
                    "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
                    "type": "string"
                  },
                  {
                    "type": "null"
                  }
                ],
                "title": "Started At"
              },
              "state": {
                "enum": [
                  "queued",
                  "running",
                  "canceling",
                  "succeeded",
                  "failed",
                  "canceled"
                ],
                "title": "State",
                "type": "string"
              }
            },
            "required": [
              "collection_id",
              "state",
              "requested_at",
              "started_at",
              "finished_at",
              "attempts",
              "result",
              "failure"
            ],
            "title": "CollectionProvenanceVerificationJobOut",
            "type": "object"
          },
          "kind": "openapi-schema",
          "schema": "CollectionProvenanceVerificationJobOut"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/verify/terminating_controls`

<!-- exact-contract-value: 654ffd6937a42b17b4204e0750fd74bd42751d2241f4a4632015efb38811a79c -->

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
        "--help"
      ]
    }
  }
]
```

</details>
