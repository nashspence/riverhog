# riverhog-ftp-adapter riverhog-ftp-adapter status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-ftp-adapter:riverhog-ftp-adapter-riverhog-ftp-adapter-status:194688f960 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-ftp-adapter` |
| Interface | `cli` |
| Family | `riverhog-ftp-adapter status` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/cli/riverhog-ftp-adapter/commands/status/name`
- `/external_contract/cli/riverhog-ftp-adapter/commands/status/parameters`

## Effective policies

- `compatibility/cli/v1`

## Executable sources and proof

- `cli:riverhog-ftp-adapter` — `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/app.py::<module>`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make dist-smoke`
- Proof: `make operation-qualification`

## Contract

- Parser name: `riverhog-ftp-adapter status`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `` | _StoreAction | no | int | --page-size |
| `` | _StoreAction | no |  | --page-token |
