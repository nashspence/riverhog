# riverhog-ftp-adapter riverhog-ftp-adapter status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-ftp-adapter:riverhog-ftp-adapter-riverhog-ftp-adapter-status:194688f960 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [cli](index.md) |
| Family | [riverhog-ftp-adapter status](index.md#f-ca752dc011) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- <a id="s-f81d8fd65a"></a>Parser name: `riverhog-ftp-adapter status`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-f54fd55b02"></a>`` | _StoreAction | no | int | --page-size |
| <a id="s-2a51aae1b2"></a>`` | _StoreAction | no |  | --page-token |

## Governing policies

- <a id="pa-482ff02048"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:riverhog-ftp-adapter](../../../evidence/sources.md#src-303f765bca) — `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/app.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

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
