# a-riverhog-filesystem-store-materialize

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-filesystem-store:a-riverhog-filesystem-store-materialize:b7eed44930 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-filesystem-store](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-3f32fb828d"></a>Parser name: `a-riverhog-filesystem-store-materialize`
- <a id="s-a39c97212d"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-1d10bd43d5"></a>`source` | required positional; 1 value | Path | not recorded |
| <a id="s-e98c5cb774"></a>`destination` | required positional; 1 value | Path | not recorded |
| <a id="s-2172b623d9"></a>`path`<br>`--path` | optional option; 1 value; collects repeats; no declared occurrence maximum | not recorded | `[]` |
| <a id="s-e3c9b3a16b"></a>`prefix`<br>`--prefix` | optional option; 1 value; collects repeats; no declared occurrence maximum | not recorded | `[]` |
| <a id="s-8870d48c0e"></a>`all_objects`<br>`--all` | optional flag; 0 values | not recorded | `false` |
| <a id="s-d5697cf9cc"></a>`json`<br>`--json` | optional flag; 0 values | not recorded | `false` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-de6391b016"></a>`help` | <a id="s-bc7606029e"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-f8c81f19af"></a>`0` | <a id="s-d83e65c8f5"></a>`"noncontractual-framework-help"` | <a id="s-7f2c9ef5e3"></a>`"empty"` |
| <a id="s-6dd4cd1649"></a>`version` | <a id="s-f81ec01d6e"></a>`{"kind":"option-present","options":["--version"]}` | <a id="s-974a45c458"></a>`0` | <a id="s-0e0f1b19a0"></a>`{"distribution":"a-riverhog-filesystem-store","kind":"installed-coordinated-release-version","serialization":"noncontractual"}` | <a id="s-850062536a"></a>`"empty"` |

### Result and failure contract

- <a id="s-4c67ce9db7"></a>Result identity: `a-riverhog-filesystem-store-materialize-cli-result/root/v1`
- <a id="s-f6ffe46970"></a>Profile: `a-riverhog-filesystem-store-materialize-cli/v1`
- <a id="s-805c8976f7"></a>Structured output: `optional-json`
- <a id="s-4c4fe9efb1"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-5f245ffb9c"></a>`completed` | <a id="s-d8fb2dbd14"></a>`{"kind":"materialization-completed"}` | <a id="s-110ae9d4a4"></a>`0` | <a id="s-59049234aa"></a>human: `"noncontractual-presentation-of-command-result"`; json: [riverhog-filesystem-materialization-result/v1](#s-1c04fbea3c) | <a id="s-bee7ef15f7"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-73cb6b8be3"></a>`usage` | <a id="s-58adf9e97c"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-67c158a03b"></a>`2` | <a id="s-f09d9d66f6"></a>all: `"empty"` | <a id="s-e5b1fd11e4"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-ac82551641"></a>`materialization` | <a id="s-cf82bb35d0"></a>`{"kind":"materialization-error"}` | <a id="s-b364f77df0"></a>`1` | <a id="s-55e71b37a3"></a>all: `"empty"` | <a id="s-0fb9372c9b"></a>all: `"noncontractual-diagnostic"` |

### Local structured outputs


#### <a id="s-1c04fbea3c"></a>`riverhog-filesystem-materialization-result/v1`

Applies to: completed · stdout (json).

<a id="s-9fc6968c01"></a>

- <a id="s-23bdc7f3cf"></a>`type`: `"object"`
- <a id="s-b8b7b969dc"></a>`additionalProperties`: `false`
- <a id="s-09f4f8e9a6"></a>`required`: `["format","destination","selected_objects","selected_bytes","source_metadata_bytes","destination_verified_objects","destination_verified_bytes","staging_verified_bytes","copied_objects","copied_bytes"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6c40e74f5b"></a>`copied_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-d6478039aa"></a>`copied_objects` | yes | type="integer"; minimum=0 |  |
| <a id="s-42ae4fba52"></a>`destination` | yes | type="string" |  |
| <a id="s-601e9df2fb"></a>`destination_verified_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-7378be954b"></a>`destination_verified_objects` | yes | type="integer"; minimum=0 |  |
| <a id="s-dc6ea5722b"></a>`format` | yes | const="riverhog-filesystem-materialization-result/v1" |  |
| <a id="s-1efe0a9ace"></a>`selected_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-21e45ae6d5"></a>`selected_objects` | yes | type="integer"; minimum=0 |  |
| <a id="s-9d3c759fb5"></a>`source_metadata_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-69f33ce447"></a>`staging_verified_bytes` | yes | type="integer"; minimum=0 |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"a-riverhog-filesystem-store-materialize"}; maximum=null; reason="no-declared-semantic-maximum"; source_constraint={"field":"kind"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --path](#s-2172b623d9) | `cardinality · occurrences · operational_policy` | shared above |
| [CLI parameter --prefix](#s-e3c9b3a16b) | `cardinality · occurrences · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter 0](#s-1d10bd43d5) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1 |
| [CLI parameter 1](#s-e98c5cb774) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1 |
| [CLI parameter --path](#s-2172b623d9) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1 |
| [CLI parameter --prefix](#s-e3c9b3a16b) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1 |
| [CLI parameter --all](#s-8870d48c0e) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0 |
| [CLI parameter --json](#s-d5697cf9cc) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0 |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-49b98a91af"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-cb3846edb7"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-5610cfe99f"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-filesystem-store-materialize](../../../evidence/sources/authorities.md#src-f31202d8f6) — [some-implementations/riverhog/storage/filesystem/src/a\_riverhog\_filesystem\_store/materialize\_cli.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/storage/filesystem/src/a_riverhog_filesystem_store/materialize_cli.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-filesystem-store-materialize/allow_abbrev`
- `/external_contract/cli/a-riverhog-filesystem-store-materialize/name`
- `/external_contract/cli/a-riverhog-filesystem-store-materialize/parameters`
- `/external_contract/cli/a-riverhog-filesystem-store-materialize/result_contract`
- `/external_contract/cli/a-riverhog-filesystem-store-materialize/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-filesystem-store-materialize/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-filesystem-store-materialize/name`

<!-- exact-contract-value: fc40ab00eb10e9ee7f65b20e6e0d42574f9d2a8f7d47b3ec51f80ab05bb77388 -->

```json
"a-riverhog-filesystem-store-materialize"
```

### `/external_contract/cli/a-riverhog-filesystem-store-materialize/parameters`

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

### `/external_contract/cli/a-riverhog-filesystem-store-materialize/result_contract`

<!-- exact-contract-value: 4eba4fd904062bac6bec5a2a74fb84fcedb4b3f72c07deac7c3b48a38429a35a -->

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
  "identity": "a-riverhog-filesystem-store-materialize-cli-result/root/v1",
  "profile_id": "a-riverhog-filesystem-store-materialize-cli/v1",
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

### `/external_contract/cli/a-riverhog-filesystem-store-materialize/terminating_controls`

<!-- exact-contract-value: d4b2bd5f29d6109a5c1bce0eb5ada925b9e3b4b35d0d31cbb34713cee68295b5 -->

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
      "distribution": "a-riverhog-filesystem-store",
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
