# riverhog-storage-adapter-filesystem-materialize

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-storage-adapter-filesystem:riverhog-storage-adapter-filesystem-materialize:c528e0a756 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-filesystem](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-220ede082e"></a>Parser name: `riverhog-storage-adapter-filesystem-materialize`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-d95ff1dd9e"></a>`source` | _StoreAction | yes | Path |  |
| <a id="s-232d4b583f"></a>`destination` | _StoreAction | yes | Path |  |
| <a id="s-19fb2fab9f"></a>`path` | _AppendAction | no |  | --path |
| <a id="s-9b1cfd448a"></a>`prefix` | _AppendAction | no |  | --prefix |
| <a id="s-8b00d9477e"></a>`all_objects` | _StoreTrueAction | no |  | --all |
| <a id="s-8400a99a20"></a>`json` | _StoreTrueAction | no |  | --json |

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
| <a id="s-87aa6be67d"></a>`completed` | <a id="s-5214dd4180"></a>`{"kind":"materialization-completed"}` | <a id="s-f75383c0b8"></a>`0` | <a id="s-cf82974b80"></a>`human: noncontractual-presentation-of-command-result; json: riverhog-filesystem-materialization-summary/v1` | <a id="s-be54da33ab"></a>`all: empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-ecfc6df42a"></a>`usage` | <a id="s-67b1b1f3d7"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-4317a5c11c"></a>`2` | <a id="s-86297cde3e"></a>`all: empty` | <a id="s-22fb428ae1"></a>`all: noncontractual-usage-diagnostic` |
| <a id="s-4ccc914638"></a>`materialization` | <a id="s-5dbfe7b0f2"></a>`{"kind":"materialization-error"}` | <a id="s-87a8cd7159"></a>`1` | <a id="s-382bb6f12c"></a>`all: empty` | <a id="s-57d17310a9"></a>`all: riverhog-filesystem-materialization-diagnostic/v1` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --all](#s-8b00d9477e) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --json](#s-8400a99a20) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-80c5b6cb3c"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-383a872168"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:riverhog-storage-adapter-filesystem-materialize](../../../evidence/sources.md#src-c89790480b) — `reference/riverhog/storage/filesystem/src/riverhog_storage_adapter_filesystem/materialize_cli.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/riverhog-storage-adapter-filesystem-materialize/name`
- `/external_contract/cli/riverhog-storage-adapter-filesystem-materialize/parameters`
- `/external_contract/cli/riverhog-storage-adapter-filesystem-materialize/result_contract`
- `/external_contract/cli/riverhog-storage-adapter-filesystem-materialize/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

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

<!-- exact-contract-value: e36b053ebd70219e22ca05bea24ca0297abf71c8e6bcfe90277a2c5d4ed4cee5 -->

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
        "all": "riverhog-filesystem-materialization-diagnostic/v1"
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
          "identity": "riverhog-filesystem-materialization-summary/v1",
          "kind": "semantic-format"
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
