# riverhog-ftp-adapter riverhog-ftp-adapter listen

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-ftp-adapter:riverhog-ftp-adapter-riverhog-ftp-adapter-listen:a80939f32a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [cli](index.md) |
| Family | [riverhog-ftp-adapter listen](index.md#f-56671c01da68) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- <a id="s-74be9905cbde"></a>Parser name: `riverhog-ftp-adapter listen`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-726cd2685bb0"></a>`` | _StoreAction | yes |  | --source |
| <a id="s-1fa0ebe49383"></a>`` | _StoreAction | yes |  | --username |
| <a id="s-1d574ff9ed70"></a>`` | _StoreAction | yes | Path | --password-file |
| <a id="s-c920a0e18030"></a>`` | _StoreAction | no |  | --host |
| <a id="s-c5f9e261117a"></a>`` | _StoreAction | no | int | --port |
| <a id="s-5ab0d6d68c50"></a>`` | _StoreAction | no | int | --passive-port-start |
| <a id="s-c3c71e786a32"></a>`` | _StoreAction | no | int | --passive-port-end |
| <a id="s-afdf255e72d9"></a>`` | _StoreAction | no |  | --public-host |
| <a id="s-ba15d7ef39f9"></a>`` | _StoreAction | no | int | --max-connections |
| <a id="s-9d17fa83705c"></a>`` | _StoreAction | no | int | --max-connections-per-ip |

## Governing policies

- <a id="pa-d393b3410eb1"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [cli:riverhog-ftp-adapter](../../../evidence/sources.md#src-303f765bca1e) — `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/app.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/riverhog-ftp-adapter/commands/listen/name`
- `/external_contract/cli/riverhog-ftp-adapter/commands/listen/parameters`

### Exact owned JSON

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
