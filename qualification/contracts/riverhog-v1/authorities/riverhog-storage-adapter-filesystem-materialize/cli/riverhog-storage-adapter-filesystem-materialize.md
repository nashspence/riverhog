# riverhog-storage-adapter-filesystem-materialize

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-storage-adapter-filesy-89bc075d61:riverhog-storage-adapter-filesystem-materialize:511ca05446 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-filesystem-materialize](../index.md) |
| Interface | [cli](index.md) |
| Family | [root](index.md#f-f4cc7aa856) |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

- <a id="s-220ede082e"></a>Parser name: `riverhog-storage-adapter-filesystem-materialize`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-d95ff1dd9e"></a>`` | _VersionAction | no |  | --version |
| <a id="s-232d4b583f"></a>`` | _StoreAction | yes | Path |  |
| <a id="s-19fb2fab9f"></a>`` | _StoreAction | yes | Path |  |
| <a id="s-9b1cfd448a"></a>`` | _AppendAction | no |  | --path |
| <a id="s-8b00d9477e"></a>`` | _AppendAction | no |  | --prefix |
| <a id="s-8400a99a20"></a>`` | _StoreTrueAction | no |  | --all |
| <a id="s-974d6e6d6f"></a>`` | _StoreTrueAction | no |  | --json |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --version](#s-d95ff1dd9e) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --all](#s-8400a99a20) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --json](#s-974d6e6d6f) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-334f3a9922"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-1b2309f714"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

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
