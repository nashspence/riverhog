# riverhog-storage-adapter-filesystem-materialize

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-storage-adapter-filesystem:riverhog-storage-adapter-filesystem-materialize:22725d19b1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-filesystem](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-220ede082e"></a>Parser name: `riverhog-storage-adapter-filesystem-materialize`
- <a id="s-be2025609c"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-d95ff1dd9e"></a>`source` | required positional; 1 value | Path | not recorded |
| <a id="s-232d4b583f"></a>`destination` | required positional; 1 value | Path | not recorded |
| <a id="s-19fb2fab9f"></a>`path`<br>`--path` | optional option; 1 value; collects repeats; no declared occurrence maximum | not recorded | `[]` |
| <a id="s-9b1cfd448a"></a>`prefix`<br>`--prefix` | optional option; 1 value; collects repeats; no declared occurrence maximum | not recorded | `[]` |
| <a id="s-8b00d9477e"></a>`all_objects`<br>`--all` | optional flag; 0 values | not recorded | `false` |
| <a id="s-8400a99a20"></a>`json`<br>`--json` | optional flag; 0 values | not recorded | `false` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-b896aec251"></a>`help` | <a id="s-dcd7c89fe8"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-df4decbe88"></a>`0` | <a id="s-c631566fe1"></a>`"noncontractual-framework-help"` | <a id="s-7106f99f9e"></a>`"empty"` |
| <a id="s-f0c8b38e7c"></a>`version` | <a id="s-7fbc5abc25"></a>`{"kind":"option-present","options":["--version"]}` | <a id="s-cba0ed7f0e"></a>`0` | <a id="s-4db5a4faa8"></a>`{"distribution":"riverhog-storage-adapter-filesystem","kind":"installed-coordinated-release-version","serialization":"noncontractual"}` | <a id="s-14f7eae9a2"></a>`"empty"` |

### Result and failure contract

- <a id="s-b995cb30b1"></a>Result identity: `riverhog-storage-adapter-filesystem-materialize-cli-result/root/v1`
- <a id="s-5ba15cdc2c"></a>Profile: `riverhog-storage-adapter-filesystem-materialize-cli/v1`
- <a id="s-79b459929f"></a>Structured output: `optional-json`
- <a id="s-d7e76a2f5f"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-87aa6be67d"></a>`completed` | <a id="s-5214dd4180"></a>`{"kind":"materialization-completed"}` | <a id="s-f75383c0b8"></a>`0` | <a id="s-cf82974b80"></a>human: `"noncontractual-presentation-of-command-result"`; json: [riverhog-filesystem-materialization-result/v1](#s-2face05c8e) | <a id="s-be54da33ab"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-ecfc6df42a"></a>`usage` | <a id="s-67b1b1f3d7"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-4317a5c11c"></a>`2` | <a id="s-86297cde3e"></a>all: `"empty"` | <a id="s-22fb428ae1"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-4ccc914638"></a>`materialization` | <a id="s-5dbfe7b0f2"></a>`{"kind":"materialization-error"}` | <a id="s-87a8cd7159"></a>`1` | <a id="s-382bb6f12c"></a>all: `"empty"` | <a id="s-57d17310a9"></a>all: `"noncontractual-diagnostic"` |

### Local structured outputs


#### <a id="s-2face05c8e"></a>`riverhog-filesystem-materialization-result/v1`

Applies to: completed · stdout (json).

<a id="s-968c42ed32"></a>

- <a id="s-fc9633f5c3"></a>`type`: `"object"`
- <a id="s-5cb88db08a"></a>`additionalProperties`: `false`
- <a id="s-136c6841ae"></a>`required`: `["format","destination","selected_objects","selected_bytes","source_metadata_bytes","destination_verified_objects","destination_verified_bytes","staging_verified_bytes","copied_objects","copied_bytes"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c5ad8bff16"></a>`copied_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-0e711e031c"></a>`copied_objects` | yes | type="integer"; minimum=0 |  |
| <a id="s-0face9d2bb"></a>`destination` | yes | type="string" |  |
| <a id="s-526ee78683"></a>`destination_verified_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-9773a4d394"></a>`destination_verified_objects` | yes | type="integer"; minimum=0 |  |
| <a id="s-e55b402099"></a>`format` | yes | const="riverhog-filesystem-materialization-result/v1" |  |
| <a id="s-db5bfa46be"></a>`selected_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-583ac4f0de"></a>`selected_objects` | yes | type="integer"; minimum=0 |  |
| <a id="s-a75a5c7bf5"></a>`source_metadata_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-1b78e45e2f"></a>`staging_verified_bytes` | yes | type="integer"; minimum=0 |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog-storage-adapter-filesystem-materialize"}; maximum=null; reason="no-declared-semantic-maximum"; source_constraint={"field":"kind"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --path](#s-19fb2fab9f) | `cardinality · occurrences · operational_policy` | shared above |
| [CLI parameter --prefix](#s-9b1cfd448a) | `cardinality · occurrences · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter 0](#s-d95ff1dd9e) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1 |
| [CLI parameter 1](#s-232d4b583f) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1 |
| [CLI parameter --path](#s-19fb2fab9f) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1 |
| [CLI parameter --prefix](#s-9b1cfd448a) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1 |
| [CLI parameter --all](#s-8b00d9477e) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0 |
| [CLI parameter --json](#s-8400a99a20) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0 |

## Governing policies

- <a id="pa-783f799413"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-c191b6e90f"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-07d672ccf9"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:riverhog-storage-adapter-filesystem-materialize](../../../evidence/sources/authorities.md#src-c89790480b) — [reference/riverhog/storage/filesystem/src/riverhog\_storage\_adapter\_filesystem/materialize\_cli.py::&lt;module&gt;](../../../../../../reference/riverhog/storage/filesystem/src/riverhog_storage_adapter_filesystem/materialize_cli.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/riverhog-storage-adapter-filesystem-materialize/allow_abbrev`
- `/external_contract/cli/riverhog-storage-adapter-filesystem-materialize/name`
- `/external_contract/cli/riverhog-storage-adapter-filesystem-materialize/parameters`
- `/external_contract/cli/riverhog-storage-adapter-filesystem-materialize/result_contract`
- `/external_contract/cli/riverhog-storage-adapter-filesystem-materialize/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/riverhog-storage-adapter-filesystem-materialize/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/riverhog-storage-adapter-filesystem-materialize/name`

<!-- exact-contract-value: 82696eaa2ee2ddb3450856c064f738d1f49e10fc6f8808ba26ca0962387019ae -->

```json
"riverhog-storage-adapter-filesystem-materialize"
```

### `/external_contract/cli/riverhog-storage-adapter-filesystem-materialize/parameters`

<!-- exact-contract-value: 3a0c17fb8fafcbad481c10cdb6a3076acac8dcaab5929f59327941ffe88ff519 -->

```json
[
  {
    "dest": "source",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [],
    "required": true,
    "type": "Path"
  },
  {
    "dest": "destination",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [],
    "required": true,
    "type": "Path"
  },
  {
    "default": [],
    "dest": "path",
    "kind": "_AppendAction",
    "nargs": null,
    "options": [
      "--path"
    ],
    "required": false
  },
  {
    "default": [],
    "dest": "prefix",
    "kind": "_AppendAction",
    "nargs": null,
    "options": [
      "--prefix"
    ],
    "required": false
  },
  {
    "default": false,
    "dest": "all_objects",
    "kind": "_StoreTrueAction",
    "nargs": 0,
    "options": [
      "--all"
    ],
    "required": false
  },
  {
    "default": false,
    "dest": "json",
    "kind": "_StoreTrueAction",
    "nargs": 0,
    "options": [
      "--json"
    ],
    "required": false
  }
]
```

### `/external_contract/cli/riverhog-storage-adapter-filesystem-materialize/result_contract`

<!-- exact-contract-value: 4dfb6fa663f7868f3986bd2d44a7e19013a52d070cc1c9f3ace663273761ddf7 -->

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
      "id": "materialization",
      "selected_by": {
        "kind": "materialization-error"
      },
      "stderr": {
        "all": "noncontractual-diagnostic"
      },
      "stdout": {
        "all": "empty"
      }
    }
  ],
  "human_json_relationship": "same-semantic-result",
  "identity": "riverhog-storage-adapter-filesystem-materialize-cli-result/root/v1",
  "profile_id": "riverhog-storage-adapter-filesystem-materialize-cli/v1",
  "structured_output": "optional-json",
  "success": [
    {
      "exit_status": 0,
      "id": "completed",
      "selected_by": {
        "kind": "materialization-completed"
      },
      "stderr": {
        "all": "empty"
      },
      "stdout": {
        "human": "noncontractual-presentation-of-command-result",
        "json": {
          "identity": "riverhog-filesystem-materialization-result/v1",
          "kind": "cli-local-json-schema",
          "schema": {
            "additionalProperties": false,
            "properties": {
              "copied_bytes": {
                "minimum": 0,
                "type": "integer"
              },
              "copied_objects": {
                "minimum": 0,
                "type": "integer"
              },
              "destination": {
                "type": "string"
              },
              "destination_verified_bytes": {
                "minimum": 0,
                "type": "integer"
              },
              "destination_verified_objects": {
                "minimum": 0,
                "type": "integer"
              },
              "format": {
                "const": "riverhog-filesystem-materialization-result/v1"
              },
              "selected_bytes": {
                "minimum": 0,
                "type": "integer"
              },
              "selected_objects": {
                "minimum": 0,
                "type": "integer"
              },
              "source_metadata_bytes": {
                "minimum": 0,
                "type": "integer"
              },
              "staging_verified_bytes": {
                "minimum": 0,
                "type": "integer"
              }
            },
            "required": [
              "format",
              "destination",
              "selected_objects",
              "selected_bytes",
              "source_metadata_bytes",
              "destination_verified_objects",
              "destination_verified_bytes",
              "staging_verified_bytes",
              "copied_objects",
              "copied_bytes"
            ],
            "type": "object"
          }
        }
      }
    }
  ]
}
```

### `/external_contract/cli/riverhog-storage-adapter-filesystem-materialize/terminating_controls`

<!-- exact-contract-value: 57a2df765e96df1f97d812b5ceab4cf9a31bc36c0c29eaa090d6299af0ea04d0 -->

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
  },
  {
    "exit_status": 0,
    "id": "version",
    "stderr": "empty",
    "stdout": {
      "distribution": "riverhog-storage-adapter-filesystem",
      "kind": "installed-coordinated-release-version",
      "serialization": "noncontractual"
    },
    "trigger": {
      "kind": "option-present",
      "options": [
        "--version"
      ]
    }
  }
]
```

</details>
