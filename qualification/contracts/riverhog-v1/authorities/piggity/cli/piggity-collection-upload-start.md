# piggity collection upload start

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-collection-upload-start:c66ea3d944 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-805df18e21"></a>Parser name: `start`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-f8d43e8cc9"></a>`root` | TyperArgument | yes | {'class': 'typer.models.TyperPath', 'name': 'path'} | root |
| <a id="s-41c9f19b64"></a>`idempotency_key` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --idempotency-key |
| <a id="s-50c5683cc5"></a>`archive_store` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --archive-store |
| <a id="s-d25200e1bc"></a>`description` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --description |
| <a id="s-02c396bde4"></a>`tag` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --tag |
| <a id="s-d85e25fde3"></a>`provenance` | TyperOption | no | {'class': 'typer.models.TyperPath', 'name': 'path'} | --provenance |
| <a id="s-0348cfd40b"></a>`omit_provenance` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --omit-provenance |
| <a id="s-598a4f2a05"></a>`provenance_observer` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --provenance-observer |
| <a id="s-9bfdcc451f"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |
| <a id="s-0cf6176bfa"></a>`dry_run` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --dry-run |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"piggity"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --tag](#s-02c396bde4) | `cardinality · occurrences · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --archive-store](#s-50c5683cc5) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --description](#s-d25200e1bc) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --dry-run](#s-0cf6176bfa) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --idempotency-key](#s-41c9f19b64) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --json](#s-9bfdcc451f) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --omit-provenance](#s-0348cfd40b) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --provenance](#s-d85e25fde3) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --provenance-observer](#s-598a4f2a05) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter root](#s-f8d43e8cc9) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --tag](#s-02c396bde4) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [GET /v1/collection-upload-sessions/{collection_id}/volumes/{volume_id}/units/{unit}](../../riverhog/http-operations/get-v1-collection-upload-sessions-collection-id-volumes-volume-id-units-unit.md)
- [GET /v1/collection-upload-sessions/{collection_id}/work](../../riverhog/http-operations/get-v1-collection-upload-sessions-collection-id-work.md)
- [GET /v1/collection-upload-sessions/{collection_id}](../../riverhog/http-operations/get-v1-collection-upload-sessions-collection-id.md)
- [POST /v1/collection-upload-sessions/{collection_id}/files](../../riverhog/http-operations/post-v1-collection-upload-sessions-collection-id-files.md)
- [POST /v1/collection-upload-sessions/{collection_id}/tags](../../riverhog/http-operations/post-v1-collection-upload-sessions-collection-id-tags.md)
- [POST /v1/collection-upload-sessions/{collection_id}/complete](../../riverhog/http-operations/post-v1-collection-upload-sessions-collection-id-complete.md)
- [POST /v1/collection-upload-sessions/{collection_id}/raw-part-digests](../../riverhog/http-operations/post-v1-collection-upload-sessions-collection-id-raw-part-digests.md)
- [POST /v1/collection-upload-sessions](../../riverhog/http-operations/post-v1-collection-upload-sessions.md)
- [PUT /v1/collection-upload-sessions/{collection_id}/volumes/{volume_id}/units/{unit}](../../riverhog/http-operations/put-v1-collection-upload-sessions-collection-id-volumes-volume-id-units-unit.md)

## Governing policies

- <a id="pa-9a80181813"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-c0de21905d"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-d0015a5cae"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

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
