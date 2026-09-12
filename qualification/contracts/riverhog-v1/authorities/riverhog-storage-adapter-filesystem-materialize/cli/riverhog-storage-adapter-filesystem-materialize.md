# riverhog-storage-adapter-filesystem-materialize

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-storage-adapter-filesy-89bc075d61:riverhog-storage-adapter-filesystem-materialize:511ca05446 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-filesystem-materialize` |
| Interface | `cli` |
| Family | `root` |
| Contract elements | 1 |
| Extent decisions | 3 |

## Machine authority

- `/external_contract/cli/riverhog-storage-adapter-filesystem-materialize/name`
- `/external_contract/cli/riverhog-storage-adapter-filesystem-materialize/parameters`

## Effective policies

- `compatibility/cli/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `cli:riverhog-storage-adapter-filesystem-materialize` — `reference/riverhog/storage/filesystem/src/riverhog_storage_adapter_filesystem/materialize_cli.py::<module>`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make dist-smoke`
- Proof: `make operation-qualification`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | values-per-occurrence | `fixed` | maximum=0, minimum=0, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=0, minimum=0, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=0, minimum=0, reason=fixed-command-argument-arity |

## Contract summary

- Parser name: `riverhog-storage-adapter-filesystem-materialize`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `` | _VersionAction | no |  | --version |
| `` | _StoreAction | yes | Path |  |
| `` | _StoreAction | yes | Path |  |
| `` | _AppendAction | no |  | --path |
| `` | _AppendAction | no |  | --prefix |
| `` | _StoreTrueAction | no |  | --all |
| `` | _StoreTrueAction | no |  | --json |

## Complete owned contract

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
