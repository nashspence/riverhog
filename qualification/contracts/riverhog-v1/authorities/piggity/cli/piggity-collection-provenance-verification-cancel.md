# piggity collection provenance verification-cancel

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-collection-provenance-verification-cancel:4770ce9a00 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-9e7a276178"></a>Parser name: `verification-cancel`
- <a id="s-eef3175688"></a>Extra arguments at this parser: rejected.
- <a id="s-e65b17e2e5"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-6a87d68b78"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-d966bbd28c"></a>`collection_id`<br>`collection_id` | required positional; 1 value | integer | not recorded<br>Env: `null` |
| <a id="s-7eca69dae1"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-8482a7df77"></a>`help` | <a id="s-6abc777dee"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-1e93783d2f"></a>`0` | <a id="s-a38f170b5a"></a>`"noncontractual-framework-help"` | <a id="s-cef1c5496e"></a>`"empty"` |

### Result and failure contract

- <a id="s-cdbc079ae3"></a>Result identity: `piggity-cli-result/collection/provenance/verification-cancel/v1`
- <a id="s-c835bce029"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-92cfd0bc9b"></a>Structured output: `optional-json`
- <a id="s-2db0b4423e"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-3d834cdbaf"></a>`completed` | <a id="s-40df70546d"></a>`{"kind":"command-completed"}` | <a id="s-26490bee49"></a>`0` | <a id="s-66bba8dad1"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP cancel_collection_provenance_verification response 200](../../riverhog/http-operations/delete-v1-collections-collection-id-provenance-verification.md#s-b9769da1e9) | <a id="s-435e7d6c31"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-8c680bed41"></a>`usage` | <a id="s-a4805d4560"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-7adde907fb"></a>`2` | <a id="s-90fa0441be"></a>all: `"empty"` | <a id="s-99f3c60d65"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-ec45fedc50"></a>`operational` | <a id="s-6154f6cb21"></a>`{"kind":"application-error"}` | <a id="s-36a90f06fc"></a>`1` | <a id="s-80cf024386"></a>human: `"empty"`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-ae17552a75"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-d966bbd28c) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-7eca69dae1) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |

## Maintained corroboration

### Related interface records

- [DELETE /v1/collections/{collection_id}/provenance/verification](../../riverhog/http-operations/delete-v1-collections-collection-id-provenance-verification.md)
- [riverhog_client.ApiClient.cancel_collection_provenance_verification](../../riverhog-client/python/riverhog-client-apiclient-cancel-collection-provenance-verification.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-70e17be81c"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-1f89d5e43f"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources/authorities.md#src-094022231f) — [reference/riverhog/applications/piggity/src/piggity/main.py::&lt;module&gt;](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [reference/riverhog/applications/piggity/src/piggity/main.py::provenance\_verification\_cancel\_cmd](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py#L2736)

### Machine authority

- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verification-cancel/allow_extra_args`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verification-cancel/allow_interspersed_args`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verification-cancel/ignore_unknown_options`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verification-cancel/name`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verification-cancel/parameters`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verification-cancel/result_contract`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verification-cancel/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verification-cancel/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verification-cancel/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verification-cancel/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verification-cancel/name`

<!-- exact-contract-value: 910bb6e80e3b26665adde72400f34b7925959fd6853bfbaa1e64f8cc2e34be5f -->

```json
"verification-cancel"
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verification-cancel/parameters`

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

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verification-cancel/result_contract`

<!-- exact-contract-value: c9fd5f6055098ec0053b7b4284ac4b292db2e41ad783f7c04bf096b11a238a33 -->

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
  "identity": "piggity-cli-result/collection/provenance/verification-cancel/v1",
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
          "method": "DELETE",
          "operation_id": "cancel_collection_provenance_verification",
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

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verification-cancel/terminating_controls`

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
