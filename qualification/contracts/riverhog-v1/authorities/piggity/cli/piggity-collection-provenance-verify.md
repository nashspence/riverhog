# piggity collection provenance verify

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-collection-provenance-verify:6d0212e99c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-49e94895c6"></a>Parser name: `verify`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-c68e7235a4"></a>`collection_id` | TyperArgument | yes | {'class': 'typer._click.types.IntParamType', 'name': 'integer'} | collection_id |
| <a id="s-d3124c5e01"></a>`wait` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --wait |
| <a id="s-68fb245f82"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Result and failure contract

- <a id="s-ca088b53e3"></a>Result identity: `piggity-cli-result/collection/provenance/verify/v1`
- <a id="s-b010ce12c3"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-3d2f177c59"></a>Structured output: `optional-json`
- <a id="s-2333b00e5e"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-809d4a2bdc"></a>`completed` | <a id="s-93b5889a73"></a>`0` | <a id="s-d3c0d01b0e"></a>`{"human":"noncontractual-presentation-of-command-result","json":"named-command-result"}` | <a id="s-c4a72e93a7"></a>`{"all":"empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-7244595aff"></a>`usage` | <a id="s-3ae9e2cf43"></a>`2` | <a id="s-b5d113e8a2"></a>`{"all":"empty"}` | <a id="s-12a53c39fd"></a>`{"all":"noncontractual-usage-diagnostic"}` |
| <a id="s-0e9abe1560"></a>`operational` | <a id="s-663bddb52d"></a>`1` | <a id="s-6f64a6c66b"></a>`{"human":"empty","json":"http-api-contracts.ErrorResponse"}` | <a id="s-2fe8a9a0a2"></a>`{"human":"noncontractual-diagnostic","json":"empty"}` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-c68e7235a4) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --json](#s-68fb245f82) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --wait](#s-d3124c5e01) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [GET /v1/collections/{collection_id}/provenance/verification](../../riverhog/http-operations/get-v1-collections-collection-id-provenance-verification.md)
- [POST /v1/collections/{collection_id}/provenance/verification](../../riverhog/http-operations/post-v1-collections-collection-id-provenance-verification.md)

## Governing policies

- <a id="pa-1113f3c66a"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-332077f35c"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verify/name`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verify/parameters`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verify/result_contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

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

<!-- exact-contract-value: 96514ceefea13c90193c35bd291ad8a0e5d17c3b2c3d2fd2bbd8e94421c6cd0f -->

```json
{
  "failures": [
    {
      "exit_status": 2,
      "id": "usage",
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
      "stderr": {
        "human": "noncontractual-diagnostic",
        "json": "empty"
      },
      "stdout": {
        "human": "empty",
        "json": "http-api-contracts.ErrorResponse"
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
      "stderr": {
        "all": "empty"
      },
      "stdout": {
        "human": "noncontractual-presentation-of-command-result",
        "json": "named-command-result"
      }
    }
  ]
}
```
