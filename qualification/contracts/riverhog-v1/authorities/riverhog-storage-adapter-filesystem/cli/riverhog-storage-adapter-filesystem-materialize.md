# riverhog-storage-adapter-filesystem-materialize

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-storage-adapter-filesystem:riverhog-storage-adapter-filesystem-materialize:79f7f79673 -->

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
| <a id="s-d95ff1dd9e"></a>`version` | _VersionAction | no |  | --version |
| <a id="s-232d4b583f"></a>`source` | _StoreAction | yes | Path |  |
| <a id="s-19fb2fab9f"></a>`destination` | _StoreAction | yes | Path |  |
| <a id="s-9b1cfd448a"></a>`path` | _AppendAction | no |  | --path |
| <a id="s-8b00d9477e"></a>`prefix` | _AppendAction | no |  | --prefix |
| <a id="s-8400a99a20"></a>`all_objects` | _StoreTrueAction | no |  | --all |
| <a id="s-974d6e6d6f"></a>`json` | _StoreTrueAction | no |  | --json |

### Result and failure contract

- <a id="s-b995cb30b1"></a>Result identity: `riverhog-storage-adapter-filesystem-materialize-cli-result/root/v1`
- <a id="s-5ba15cdc2c"></a>Profile: `riverhog-storage-adapter-filesystem-materialize-cli/v1`
- <a id="s-79b459929f"></a>Structured output: `optional-json`
- <a id="s-d7e76a2f5f"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-87aa6be67d"></a>`completed` | <a id="s-f75383c0b8"></a>`0` | <a id="s-cf82974b80"></a>`{"human":"noncontractual-presentation-of-command-result","json":"riverhog-filesystem-materialization-summary/v1"}` | <a id="s-be54da33ab"></a>`{"all":"empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-ecfc6df42a"></a>`usage` | <a id="s-4317a5c11c"></a>`2` | <a id="s-86297cde3e"></a>`{"all":"empty"}` | <a id="s-22fb428ae1"></a>`{"all":"noncontractual-usage-diagnostic"}` |
| <a id="s-4ccc914638"></a>`materialization` | <a id="s-87a8cd7159"></a>`1` | <a id="s-382bb6f12c"></a>`{"all":"empty"}` | <a id="s-57d17310a9"></a>`{"all":"riverhog-filesystem-materialization-diagnostic/v1"}` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --version](#s-d95ff1dd9e) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --all](#s-8400a99a20) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --json](#s-974d6e6d6f) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-545ec076de"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-23c5f7a20c"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

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

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/riverhog-storage-adapter-filesystem-materialize/name`

<!-- exact-contract-value: 82696eaa2ee2ddb3450856c064f738d1f49e10fc6f8808ba26ca0962387019ae -->

```json
"riverhog-storage-adapter-filesystem-materialize"
```

### `/external_contract/cli/riverhog-storage-adapter-filesystem-materialize/parameters`

<!-- exact-contract-value: f8a7ef39da8b15ad5f557610d25478cad6509560fa0249ed688e23cc17e9cc01 -->

```json
[
  {
    "dest": "version",
    "kind": "_VersionAction",
    "nargs": 0,
    "options": [
      "--version"
    ],
    "required": false
  },
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

<!-- exact-contract-value: 9c4d2dd31864aa013a97846fa19fe91e20ae83f2bd41c77356c1cb50a7a994dc -->

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
      "id": "materialization",
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
      "stderr": {
        "all": "empty"
      },
      "stdout": {
        "human": "noncontractual-presentation-of-command-result",
        "json": "riverhog-filesystem-materialization-summary/v1"
      }
    }
  ]
}
```
