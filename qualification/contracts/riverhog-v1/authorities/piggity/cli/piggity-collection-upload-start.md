# piggity collection upload start

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-collection-upload-start:c66ea3d944 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [cli](index.md) |
| Family | [collection](families/collection/index.md) |
| Contract elements | 1 |
| Extent decisions | 11 |

## External contract

- <a id="s-805df18e2114"></a>Parser name: `start`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-f8d43e8cc998"></a>`root` | TyperArgument | yes | {'class': 'typer.models.TyperPath', 'name': 'path'} | root |
| <a id="s-41c9f19b64ab"></a>`idempotency_key` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --idempotency-key |
| <a id="s-50c5683cc591"></a>`archive_store` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --archive-store |
| <a id="s-d25200e1bc02"></a>`description` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --description |
| <a id="s-02c396bde4b6"></a>`tag` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --tag |
| <a id="s-d85e25fde390"></a>`provenance` | TyperOption | no | {'class': 'typer.models.TyperPath', 'name': 'path'} | --provenance |
| <a id="s-0348cfd40bf2"></a>`omit_provenance` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --omit-provenance |
| <a id="s-598a4f2a05ec"></a>`provenance_observer` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --provenance-observer |
| <a id="s-9bfdcc451f8a"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |
| <a id="s-0cf6176bfa8a"></a>`dry_run` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --dry-run |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"piggity"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --tag](#s-02c396bde4b6) | `cardinality · occurrences · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --archive-store](#s-50c5683cc591) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --description](#s-d25200e1bc02) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --dry-run](#s-0cf6176bfa8a) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --idempotency-key](#s-41c9f19b64ab) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --json](#s-9bfdcc451f8a) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --omit-provenance](#s-0348cfd40bf2) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --provenance](#s-d85e25fde390) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --provenance-observer](#s-598a4f2a05ec) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter root](#s-f8d43e8cc998) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --tag](#s-02c396bde4b6) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [Operation parity: create_or_resume_collection_upload_session](../../riverhog/operation/operation-parity-create-or-resume-collection-upload-session.md)
- [Operation parity: get_collection_upload_session](../../riverhog/operation/operation-parity-get-collection-upload-session.md)
- [Operation parity: complete_collection_upload_session](../../riverhog/operation/operation-parity-complete-collection-upload-session.md)
- [Operation parity: register_collection_upload_session_files](../../riverhog/operation/operation-parity-register-collection-upload-session-files.md)
- [Operation parity: register_collection_upload_session_raw_part_digests](../../riverhog/operation/operation-parity-register-collection-upload-session-raw-part-digests.md)
- [Operation parity: add_collection_upload_session_tags](../../riverhog/operation/operation-parity-add-collection-upload-session-tags.md)
- [Operation parity: get_collection_upload_session_unit](../../riverhog/operation/operation-parity-get-collection-upload-session-unit.md)
- [Operation parity: put_collection_upload_session_unit](../../riverhog/operation/operation-parity-put-collection-upload-session-unit.md)
- [Operation parity: acquire_collection_upload_session_work](../../riverhog/operation/operation-parity-acquire-collection-upload-session-work.md)

## Governing policies

- <a id="pa-9a801818134d"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-c0de21905dac"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)
- <a id="pa-d0015a5caecc"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f2c) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/collection/commands/upload/commands/start/name`
- `/external_contract/cli/piggity/commands/collection/commands/upload/commands/start/parameters`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/collection/commands/upload/commands/start/name`

<!-- exact-contract-value: a92ae9615600f7f0bcb0edf9703b379c163bef33ed749ae40c48a0830d4ab6ae -->

```json
"start"
```

### `/external_contract/cli/piggity/commands/collection/commands/upload/commands/start/parameters`

<!-- exact-contract-value: adb95542af19e382535840736acd6066011502a1227af6e297b20b882ba2c3be -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "root",
    "nargs": 1,
    "options": [
      "root"
    ],
    "required": true,
    "secondary_options": [],
    "type": {
      "class": "typer.models.TyperPath",
      "name": "path"
    }
  },
  {
    "count": false,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "idempotency_key",
    "nargs": 1,
    "options": [
      "--idempotency-key"
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
    "name": "archive_store",
    "nargs": 1,
    "options": [
      "--archive-store"
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
    "name": "description",
    "nargs": 1,
    "options": [
      "--description"
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
    "multiple": true,
    "name": "tag",
    "nargs": 1,
    "options": [
      "--tag"
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
    "name": "provenance",
    "nargs": 1,
    "options": [
      "--provenance"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer.models.TyperPath",
      "name": "path"
    }
  },
  {
    "count": false,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "omit_provenance",
    "nargs": 1,
    "options": [
      "--omit-provenance"
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
    "envvar": "PIGGITY_PROVENANCE_OBSERVER",
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "provenance_observer",
    "nargs": 1,
    "options": [
      "--provenance-observer"
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
  },
  {
    "count": false,
    "default": false,
    "envvar": null,
    "is_flag": true,
    "kind": "TyperOption",
    "multiple": false,
    "name": "dry_run",
    "nargs": 1,
    "options": [
      "--dry-run"
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
