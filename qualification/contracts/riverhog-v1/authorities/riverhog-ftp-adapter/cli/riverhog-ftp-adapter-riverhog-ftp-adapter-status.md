# riverhog-ftp-adapter riverhog-ftp-adapter status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-ftp-adapter:riverhog-ftp-adapter-riverhog-ftp-adapter-status:194688f960 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog-ftp-adapter` |
| Interface | `cli` |
| Family | `riverhog-ftp-adapter status` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- Parser name: `riverhog-ftp-adapter status`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `` | _StoreAction | no | int | --page-size |
| `` | _StoreAction | no |  | --page-token |

## Governing policies

- `compatibility/cli/v1`

## Evidence

### Qualification

- `make dist-smoke`
- `make operation-qualification`

### Executable sources

- `cli:riverhog-ftp-adapter` — `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/app.py::<module>`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/riverhog-ftp-adapter/commands/status/name`
- `/external_contract/cli/riverhog-ftp-adapter/commands/status/parameters`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/riverhog-ftp-adapter/commands/status/name`

<!-- exact-contract-value: 4f8647d30fce9b7e9c446594e1435541adbe63741db99e75cdb55efca7fd3930 -->

```json
"riverhog-ftp-adapter status"
```

### `/external_contract/cli/riverhog-ftp-adapter/commands/status/parameters`

<!-- exact-contract-value: 9c78a4249746cc77ff1ad2537e381ac766e9b1e5866cda7e926f55bfb3973828 -->

```json
[
  {
    "default": 25,
    "dest": "page_size",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--page-size"
    ],
    "required": false,
    "type": "int"
  },
  {
    "dest": "page_token",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--page-token"
    ],
    "required": false
  }
]
```
