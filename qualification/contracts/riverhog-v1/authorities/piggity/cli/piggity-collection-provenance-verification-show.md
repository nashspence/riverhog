# piggity collection provenance verification-show

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-collection-provenance-verification-show:cb985fd204 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-1521ac953f"></a>Parser name: `verification-show`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-157365010e"></a>`collection_id` | TyperArgument | yes | {'class': 'typer._click.types.IntParamType', 'name': 'integer'} | collection_id |
| <a id="s-f117275074"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

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
| <a id="s-8a590322bd"></a>`completed` | <a id="s-a17562f0ac"></a>`{"kind":"command-completed"}` | <a id="s-772cf26ed7"></a>`0` | <a id="s-3ef47693ca"></a>`human: noncontractual-presentation-of-command-result; json: HTTP get_collection_provenance_verification — #/components/schemas/CollectionProvenanceVerificationJobOut` | <a id="s-dbcd8b3665"></a>`all: empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-6ef6a5af4b"></a>`usage` | <a id="s-93601c9e09"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-77b1634441"></a>`2` | <a id="s-e110e9883d"></a>`all: empty` | <a id="s-c1d75671ba"></a>`all: noncontractual-usage-diagnostic` |
| <a id="s-af84fce3a6"></a>`operational` | <a id="s-e3d481b2b0"></a>`{"kind":"application-error"}` | <a id="s-012697735c"></a>`1` | <a id="s-8b04f57a99"></a>`human: empty; json: http-api-contracts.ErrorResponse` | <a id="s-45bb0945d5"></a>`human: noncontractual-diagnostic; json: empty` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-157365010e) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --json](#s-f117275074) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [GET /v1/collections/{collection_id}/provenance/verification](../../riverhog/http-operations/get-v1-collections-collection-id-provenance-verification.md)

## Governing policies

- <a id="pa-0f4207a78b"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-5ad502c6aa"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verification-show/name`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verification-show/parameters`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verification-show/result_contract`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verification-show/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

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
