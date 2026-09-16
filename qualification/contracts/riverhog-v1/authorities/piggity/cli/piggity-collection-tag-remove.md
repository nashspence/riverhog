# piggity collection tag remove

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-collection-tag-remove:9ff7f403bd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-100a8de93d"></a>Parser name: `remove`
- <a id="s-816740483c"></a>Extra arguments at this parser: rejected.
- <a id="s-e07c91fbc9"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-9e6d4bf5cf"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-994c7acfc4"></a>`collection_id`<br>`collection_id` | required positional; 1 value | integer | not recorded |
| <a id="s-01caf1ef16"></a>`tag`<br>`tag` | required positional; 1 value | text | not recorded |
| <a id="s-cf4f590958"></a>`revision`<br>`--revision` | optional option; 1 value | integer range; minimum=`1` (inclusive); outside range: reject | not recorded |
| <a id="s-fb180fbdab"></a>`tag_set_identity`<br>`--tag-set-identity` | optional option; 1 value | text | not recorded |
| <a id="s-2c49b4996b"></a>`operation_id`<br>`--operation-id` | optional option; 1 value | text | not recorded |
| <a id="s-ecd83e721f"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-a236a0b92b"></a>`help` | <a id="s-2990650a74"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-12f24396ad"></a>`0` | <a id="s-6fe6f5b3fe"></a>`"noncontractual-framework-help"` | <a id="s-78221e5e65"></a>`"empty"` |

### Result and failure contract

- <a id="s-d8652cfae5"></a>Result identity: `piggity-cli-result/collection/tag/remove/v1`
- <a id="s-47908f5cf6"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-f31adccc8a"></a>Structured output: `optional-json`
- <a id="s-361868bd9b"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-79a0930f60"></a>`completed` | <a id="s-0b9aea87d2"></a>`{"kind":"command-completed"}` | <a id="s-52766ae6db"></a>`0` | <a id="s-bd5b7ba211"></a>human: `noncontractual-presentation-of-command-result`; json: [HTTP remove_collection_tag response 200](../../riverhog/http-operations/post-v1-collections-collection-id-tags-remove.md#s-ea545b124d) | <a id="s-acb6eba5bf"></a>all: `empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-88fc3ada2c"></a>`usage` | <a id="s-6aad6cc09e"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-c34ecccce7"></a>`2` | <a id="s-956c4c9be8"></a>all: `empty` | <a id="s-c2232b9412"></a>all: `noncontractual-usage-diagnostic` |
| <a id="s-f22681ab50"></a>`operational` | <a id="s-7dcdc0c9c6"></a>`{"kind":"application-error"}` | <a id="s-9db791b331"></a>`1` | <a id="s-a0ae7de968"></a>human: `empty`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-e078959ad2"></a>human: `noncontractual-diagnostic`; json: `empty` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-994c7acfc4) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-ecd83e721f) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter --operation-id](#s-2c49b4996b) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --revision](#s-cf4f590958) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter tag](#s-01caf1ef16) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --tag-set-identity](#s-fb180fbdab) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [GET /v1/collections/{collection_id}](../../riverhog/http-operations/get-v1-collections-collection-id.md)
- [POST /v1/collections/{collection_id}/tags:remove](../../riverhog/http-operations/post-v1-collections-collection-id-tags-remove.md)
- [riverhog_client.ApiClient.get_collection](../../riverhog-client/python/riverhog-client-apiclient-get-collection.md)
- [riverhog_client.ApiClient.remove_collection_tag](../../riverhog-client/python/riverhog-client-apiclient-remove-collection-tag.md)

## Governing policies

- <a id="pa-0a88143a26"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-3197881e58"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **Command callback:** [reference/riverhog/applications/piggity/src/piggity/main.py::collection_tag_remove_cmd](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py#L1086)

### Machine authority

- `/external_contract/cli/piggity/commands/collection/commands/tag/commands/remove/allow_extra_args`
- `/external_contract/cli/piggity/commands/collection/commands/tag/commands/remove/allow_interspersed_args`
- `/external_contract/cli/piggity/commands/collection/commands/tag/commands/remove/ignore_unknown_options`
- `/external_contract/cli/piggity/commands/collection/commands/tag/commands/remove/name`
- `/external_contract/cli/piggity/commands/collection/commands/tag/commands/remove/parameters`
- `/external_contract/cli/piggity/commands/collection/commands/tag/commands/remove/result_contract`
- `/external_contract/cli/piggity/commands/collection/commands/tag/commands/remove/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/collection/commands/tag/commands/remove/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/collection/commands/tag/commands/remove/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/collection/commands/tag/commands/remove/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/collection/commands/tag/commands/remove/name`

<!-- exact-contract-value: 66f3d68b7627c9f6965029d01923daefa869b3f33123c145ddb25464b41a529a -->

```json
"remove"
```

### `/external_contract/cli/piggity/commands/collection/commands/tag/commands/remove/parameters`

<!-- exact-contract-value: 23024ad83c5fd41b7ac0b5f06d52336cf0e25f1fdef5696c32f25fa253c71943 -->

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
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "tag",
    "nargs": 1,
    "options": [
      "tag"
    ],
    "required": true,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.StringParamType",
      "name": "text"
    }
  },
  {
    "count": false,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "revision",
    "nargs": 1,
    "options": [
      "--revision"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "clamp": false,
      "class": "typer._click.types.IntRange",
      "max_open": false,
      "min_open": false,
      "minimum": 1,
      "name": "integer range"
    }
  },
  {
    "count": false,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "tag_set_identity",
    "nargs": 1,
    "options": [
      "--tag-set-identity"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.StringParamType",
      "name": "text"
    }
  },
  {
    "count": false,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "operation_id",
    "nargs": 1,
    "options": [
      "--operation-id"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.StringParamType",
      "name": "text"
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

### `/external_contract/cli/piggity/commands/collection/commands/tag/commands/remove/result_contract`

<!-- exact-contract-value: c0f4ffc4a96131baed775a59db248e3bac033214197d69c0f8396166aefbe63e -->

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
  "identity": "piggity-cli-result/collection/tag/remove/v1",
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
          "method": "POST",
          "operation_id": "remove_collection_tag",
          "path": "/v1/collections/{collection_id}/tags:remove",
          "schema": {
            "$ref": "#/components/schemas/CollectionTagMutationOut"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/piggity/commands/collection/commands/tag/commands/remove/terminating_controls`

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
