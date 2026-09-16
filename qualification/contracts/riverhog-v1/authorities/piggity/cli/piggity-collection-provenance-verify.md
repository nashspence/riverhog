# piggity collection provenance verify

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-collection-provenance-verify:dc8ed7bd61 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-49e94895c6"></a>Parser name: `verify`
- <a id="s-907054e82e"></a>Extra arguments at this parser: rejected.
- <a id="s-2977dfa282"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-33fa32524a"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-c68e7235a4"></a>`collection_id`<br>`collection_id` | required positional; 1 value | integer | not recorded |
| <a id="s-d3124c5e01"></a>`wait`<br>`--wait`, alternate: `--no-wait` | optional flag; 0 values | boolean | `true` |
| <a id="s-68fb245f82"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-d410b937ab"></a>`help` | <a id="s-a9f9d26258"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-c2ebffc2bf"></a>`0` | <a id="s-cee9000df7"></a>`"noncontractual-framework-help"` | <a id="s-0913dd16c1"></a>`"empty"` |

### Result and failure contract

- <a id="s-ca088b53e3"></a>Result identity: `piggity-cli-result/collection/provenance/verify/v1`
- <a id="s-b010ce12c3"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-3d2f177c59"></a>Structured output: `optional-json`
- <a id="s-2333b00e5e"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-809d4a2bdc"></a>`completed` | <a id="s-071baef8ae"></a>`{"kind":"command-completed"}` | <a id="s-93b5889a73"></a>`0` | <a id="s-d3c0d01b0e"></a>human: `noncontractual-presentation-of-command-result`; json: [OpenAPI riverhog.CollectionProvenanceVerificationJobOut](../../riverhog/http-schemas/schemas-collectionprovenanceverificationjobout.md) | <a id="s-c4a72e93a7"></a>all: `empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-7244595aff"></a>`usage` | <a id="s-ba125b5e0f"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-3ae9e2cf43"></a>`2` | <a id="s-b5d113e8a2"></a>all: `empty` | <a id="s-12a53c39fd"></a>all: `noncontractual-usage-diagnostic` |
| <a id="s-0e9abe1560"></a>`operational` | <a id="s-9ae8852fac"></a>`{"kind":"application-error"}` | <a id="s-663bddb52d"></a>`1` | <a id="s-6f64a6c66b"></a>human: `empty`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-2fe8a9a0a2"></a>human: `noncontractual-diagnostic`; json: `empty` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-c68e7235a4) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-68fb245f82) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter --wait](#s-d3124c5e01) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |

## Maintained corroboration

### Related interface records

- [GET /v1/collections/{collection_id}/provenance/verification](../../riverhog/http-operations/get-v1-collections-collection-id-provenance-verification.md)
- [POST /v1/collections/{collection_id}/provenance/verification](../../riverhog/http-operations/post-v1-collections-collection-id-provenance-verification.md)
- [riverhog_client.ApiClient.get_collection_provenance_verification](../../riverhog-client/python/riverhog-client-apiclient-get-collection-provenance-verification.md)
- [riverhog_client.ApiClient.request_collection_provenance_verification](../../riverhog-client/python/riverhog-client-apiclient-request-collection-provenance-verification.md)

## Governing policies

- <a id="pa-cd0b0ce8ba"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-e963c55740"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **Command callback:** [reference/riverhog/applications/piggity/src/piggity/main.py::provenance_verify_cmd](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py#L2706)

### Machine authority

- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verify/allow_extra_args`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verify/allow_interspersed_args`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verify/ignore_unknown_options`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verify/name`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verify/parameters`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verify/result_contract`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verify/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verify/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verify/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verify/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verify/name`

<!-- exact-contract-value: 898c74c2eed0452b1e51e567f237c37f1caa1e52f747466d56e76e15d07dc331 -->

```json
"verify"
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verify/parameters`

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

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verify/result_contract`

<!-- exact-contract-value: 0e90fc2867f74d19f2a5cee7b2146897a54de50af49892233a3f458d81898b13 -->

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
  "identity": "piggity-cli-result/collection/provenance/verify/v1",
  "profile_id": "piggity-cli-human-json/v1",
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
                    "type": "string"
                  },
                  {
                    "type": "null"
                  }
                ],
                "title": "Finished At"
              },
              "requested_at": {
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

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verify/terminating_controls`

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
