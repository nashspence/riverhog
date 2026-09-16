# piggity app key revoke

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-app-key-revoke:9d9d6e4632 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-9c780f52a5"></a>Parser name: `revoke`

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-6c6eb4f988"></a>`app_name`<br>`app_name` | required positional; 1 value | text | not recorded |
| <a id="s-15b43c36fc"></a>`key_id`<br>`key_id` | required positional; 1 value | text | not recorded |
| <a id="s-96ecefa133"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-40df9f03f3"></a>`help` | <a id="s-59faed1796"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-d7a56440d4"></a>`0` | <a id="s-91dd94efd7"></a>`"noncontractual-framework-help"` | <a id="s-ee7e2844ad"></a>`"empty"` |

### Result and failure contract

- <a id="s-76df4c7cb3"></a>Result identity: `piggity-cli-result/app/key/revoke/v1`
- <a id="s-d9c9798dbf"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-8e944e6385"></a>Structured output: `optional-json`
- <a id="s-8104562953"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-5287729f12"></a>`completed` | <a id="s-07fcfada21"></a>`{"kind":"command-completed"}` | <a id="s-369a035560"></a>`0` | <a id="s-049336a602"></a>human: `noncontractual-presentation-of-command-result`; json: [HTTP revoke_app_key response 200](../../riverhog/http-operations/post-v1-apps-app-keys-key-id-revoke.md#s-bb1d20bb4c) | <a id="s-ff4b62f10e"></a>all: `empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-eacc493597"></a>`usage` | <a id="s-6a76e1d4f8"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-a3f90aae8f"></a>`2` | <a id="s-a8bfda6ceb"></a>all: `empty` | <a id="s-bd7ddce154"></a>all: `noncontractual-usage-diagnostic` |
| <a id="s-c096dffa60"></a>`operational` | <a id="s-73678d6a06"></a>`{"kind":"application-error"}` | <a id="s-6181f7aa27"></a>`1` | <a id="s-2fa2065ffa"></a>human: `empty`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-bdc4f8c32f"></a>human: `noncontractual-diagnostic`; json: `empty` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter app_name](#s-6c6eb4f988) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-96ecefa133) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter key_id](#s-15b43c36fc) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [POST /v1/apps/{app}/keys/{key_id}/revoke](../../riverhog/http-operations/post-v1-apps-app-keys-key-id-revoke.md)
- [riverhog_client.ApiClient.revoke_app_key](../../riverhog-client/python/riverhog-client-apiclient-revoke-app-key.md)

## Governing policies

- <a id="pa-8cc9c0a91d"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-8a45bb89d9"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **Command callback:** [reference/riverhog/applications/piggity/src/piggity/main.py::app_key_revoke_cmd](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py#L1209)

### Machine authority

- `/external_contract/cli/piggity/commands/app/commands/key/commands/revoke/name`
- `/external_contract/cli/piggity/commands/app/commands/key/commands/revoke/parameters`
- `/external_contract/cli/piggity/commands/app/commands/key/commands/revoke/result_contract`
- `/external_contract/cli/piggity/commands/app/commands/key/commands/revoke/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/app/commands/key/commands/revoke/name`

<!-- exact-contract-value: 4dbb5115705a0de2a6127443c67e751a0baa58326bad39f890b1efc796cc6a3a -->

```json
"revoke"
```

### `/external_contract/cli/piggity/commands/app/commands/key/commands/revoke/parameters`

<!-- exact-contract-value: 1749f130449a17b41843bc114da325f40e7d6ade60854043985e5bcec11d2cad -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "app_name",
    "nargs": 1,
    "options": [
      "app_name"
    ],
    "required": true,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.StringParamType",
      "name": "text"
    }
  },
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "key_id",
    "nargs": 1,
    "options": [
      "key_id"
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

### `/external_contract/cli/piggity/commands/app/commands/key/commands/revoke/result_contract`

<!-- exact-contract-value: 805486d3e5a9592dfb11f1908dbf2f00e73b1956d10c0dbd543f158e1da50db1 -->

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
  "identity": "piggity-cli-result/app/key/revoke/v1",
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
          "operation_id": "revoke_app_key",
          "path": "/v1/apps/{app}/keys/{key_id}/revoke",
          "schema": {
            "$ref": "#/components/schemas/AppKeyOut"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/piggity/commands/app/commands/key/commands/revoke/terminating_controls`

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
