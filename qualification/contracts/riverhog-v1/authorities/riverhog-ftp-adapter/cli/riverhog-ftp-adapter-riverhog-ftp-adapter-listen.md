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

## Contract summary

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

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/riverhog-ftp-adapter/commands/listen/name`

<!-- exact-contract-value: a2ccfda5d687b219e3ef53a921b97daa9b6add6d4150c3ab5a402b8f88a64a21 -->

```json
"riverhog-ftp-adapter listen"
```

### `/external_contract/cli/riverhog-ftp-adapter/commands/listen/parameters`

<!-- exact-contract-value: 16bec56e9da277c3cfdde2175af9cafb1d17d696bd2668c018e041094e381aac -->

```json
[
  {
    "dest": "source",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--source"
    ],
    "required": true
  },
  {
    "dest": "username",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--username"
    ],
    "required": true
  },
  {
    "dest": "password_file",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--password-file"
    ],
    "required": true,
    "type": "Path"
  },
  {
    "default": "127.0.0.1",
    "dest": "host",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--host"
    ],
    "required": false
  },
  {
    "default": 2121,
    "dest": "port",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--port"
    ],
    "required": false,
    "type": "int"
  },
  {
    "default": 30000,
    "dest": "passive_port_start",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--passive-port-start"
    ],
    "required": false,
    "type": "int"
  },
  {
    "default": 30039,
    "dest": "passive_port_end",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--passive-port-end"
    ],
    "required": false,
    "type": "int"
  },
  {
    "dest": "public_host",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--public-host"
    ],
    "required": false
  },
  {
    "default": 256,
    "dest": "max_connections",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--max-connections"
    ],
    "required": false,
    "type": "int"
  },
  {
    "default": 32,
    "dest": "max_connections_per_ip",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--max-connections-per-ip"
    ],
    "required": false,
    "type": "int"
  }
]
```
