# riverhog-ftp-adapter riverhog-ftp-adapter listen

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-ftp-adapter:riverhog-ftp-adapter-riverhog-ftp-adapter-listen:a80939f32a -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-ftp-adapter` |
| Interface | `cli` |
| Family | `riverhog-ftp-adapter listen` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/cli/riverhog-ftp-adapter/commands/listen/name`
- `/external_contract/cli/riverhog-ftp-adapter/commands/listen/parameters`

## Effective policies

- `compatibility/cli/v1`

## Executable sources and proof

- `cli:riverhog-ftp-adapter` — `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/app.py::<module>`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make dist-smoke`
- Proof: `make operation-qualification`

## Contract

- Parser name: `riverhog-ftp-adapter listen`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `` | _StoreAction | yes |  | --source |
| `` | _StoreAction | yes |  | --username |
| `` | _StoreAction | yes | Path | --password-file |
| `` | _StoreAction | no |  | --host |
| `` | _StoreAction | no | int | --port |
| `` | _StoreAction | no | int | --passive-port-start |
| `` | _StoreAction | no | int | --passive-port-end |
| `` | _StoreAction | no |  | --public-host |
| `` | _StoreAction | no | int | --max-connections |
| `` | _StoreAction | no | int | --max-connections-per-ip |
