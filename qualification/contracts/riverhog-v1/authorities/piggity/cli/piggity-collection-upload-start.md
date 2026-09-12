# piggity collection upload start

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-collection-upload-start:c66ea3d944 -->

| Audit field | Value |
|---|---|
| Authority | `piggity` |
| Interface | `cli` |
| Family | `collection` |
| Contract elements | 1 |
| Extent decisions | 11 |

## Machine authority

- `/external_contract/cli/piggity/commands/collection/commands/upload/commands/start/name`
- `/external_contract/cli/piggity/commands/collection/commands/upload/commands/start/parameters`

## Effective policies

- `compatibility/cli/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `cli:piggity` — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make dist-smoke`
- Proof: `make operation-qualification`

## Related interface records

- [Operation parity: create_or_resume_collection_upload_session](../../riverhog/operation/operation-parity-create-or-resume-collection-upload-session.md)
- [Operation parity: get_collection_upload_session](../../riverhog/operation/operation-parity-get-collection-upload-session.md)
- [Operation parity: complete_collection_upload_session](../../riverhog/operation/operation-parity-complete-collection-upload-session.md)
- [Operation parity: register_collection_upload_session_files](../../riverhog/operation/operation-parity-register-collection-upload-session-files.md)
- [Operation parity: register_collection_upload_session_raw_part_digests](../../riverhog/operation/operation-parity-register-collection-upload-session-raw-part-digests.md)
- [Operation parity: add_collection_upload_session_tags](../../riverhog/operation/operation-parity-add-collection-upload-session-tags.md)
- [Operation parity: get_collection_upload_session_unit](../../riverhog/operation/operation-parity-get-collection-upload-session-unit.md)
- [Operation parity: put_collection_upload_session_unit](../../riverhog/operation/operation-parity-put-collection-upload-session-unit.md)
- [Operation parity: acquire_collection_upload_session_work](../../riverhog/operation/operation-parity-acquire-collection-upload-session-work.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | occurrences | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |

## Contract

- Parser name: `start`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `root` | TyperArgument | yes | {'class': 'typer.models.TyperPath', 'name': 'path'} | root |
| `idempotency_key` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --idempotency-key |
| `archive_store` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --archive-store |
| `description` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --description |
| `tag` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --tag |
| `provenance` | TyperOption | no | {'class': 'typer.models.TyperPath', 'name': 'path'} | --provenance |
| `omit_provenance` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --omit-provenance |
| `provenance_observer` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --provenance-observer |
| `json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |
| `dry_run` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --dry-run |
