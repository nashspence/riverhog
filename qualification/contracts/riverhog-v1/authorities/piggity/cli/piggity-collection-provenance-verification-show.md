# piggity collection provenance verification-show

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-collection-provenance-verification-show:4d613b1868 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-1521ac953f"></a>Parser name: `verification-show`
- <a id="s-04d6f57fa9"></a>Extra arguments at this parser: rejected.
- <a id="s-40aaf95241"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-c90f6762e1"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-157365010e"></a>`collection_id`<br>`collection_id` | required positional; 1 value | integer | not recorded<br>Env: `null` |
| <a id="s-f117275074"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-d06739316c"></a>`help` | <a id="s-ca658412da"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-725dd1699b"></a>`0` | <a id="s-c99876599c"></a>`"noncontractual-framework-help"` | <a id="s-041bfe2646"></a>`"empty"` |

### Result and failure contract

- <a id="s-5d12492880"></a>Result identity: `piggity-cli-result/collection/provenance/verification-show/v1`
- <a id="s-82fcb992e4"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-a84c9279cb"></a>Structured output: `optional-json`
- <a id="s-e0be41eb3e"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-8a590322bd"></a>`completed` | <a id="s-a17562f0ac"></a>`{"kind":"command-completed"}` | <a id="s-772cf26ed7"></a>`0` | <a id="s-3ef47693ca"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP get_collection_provenance_verification response 200](../../riverhog/http-operations/get-v1-collections-collection-id-provenance-verification.md#s-d1f90bc6b1) | <a id="s-dbcd8b3665"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-6ef6a5af4b"></a>`usage` | <a id="s-93601c9e09"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-77b1634441"></a>`2` | <a id="s-e110e9883d"></a>all: `"empty"` | <a id="s-c1d75671ba"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-af84fce3a6"></a>`operational` | <a id="s-e3d481b2b0"></a>`{"kind":"application-error"}` | <a id="s-012697735c"></a>`1` | <a id="s-8b04f57a99"></a>human: `"empty"`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-45bb0945d5"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-157365010e) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-f117275074) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |

## Maintained corroboration

### Related interface records

- [GET /v1/collections/{collection_id}/provenance/verification](../../riverhog/http-operations/get-v1-collections-collection-id-provenance-verification.md)
- [riverhog_client.ApiClient.get_collection_provenance_verification](../../riverhog-client/python/riverhog-client-apiclient-get-collection-provenance-verification.md)

## Governing policies

- <a id="pa-4f1e86f98e"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-c293ad94fa"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **Command callback:** [reference/riverhog/applications/piggity/src/piggity/main.py::provenance_verification_show_cmd](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py#L2725)

### Machine authority

- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verification-show/allow_extra_args`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verification-show/allow_interspersed_args`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verification-show/ignore_unknown_options`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verification-show/name`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verification-show/parameters`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verification-show/result_contract`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verification-show/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verification-show/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verification-show/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verification-show/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verification-show/name`

<!-- exact-contract-value: 2ad855165a84772f590f6bdd471c8bce64b16730e7186147c2b558c1f38974da -->

```json
"verification-show"
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verification-show/parameters`

<!-- exact-contract-value: 69121b7dd4df39852c314f302ca34fb358e3d4472f9e5565bdec50242e30ee3a -->

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

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verification-show/result_contract`

<!-- exact-contract-value: de33bf0a91aae4bac4205880564bb71bb2278410f1b860b7ce2b8ac88dc4da50 -->

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
  "identity": "piggity-cli-result/collection/provenance/verification-show/v1",
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
          "kind": "http-operation-response",
          "method": "GET",
          "operation_id": "get_collection_provenance_verification",
          "path": "/v1/collections/{collection_id}/provenance/verification",
          "schema": {
            "$ref": "#/components/schemas/CollectionProvenanceVerificationJobOut"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verification-show/terminating_controls`

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
