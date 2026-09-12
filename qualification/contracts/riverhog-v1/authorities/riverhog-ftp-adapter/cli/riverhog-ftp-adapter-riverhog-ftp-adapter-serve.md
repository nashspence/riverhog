# riverhog-ftp-adapter riverhog-ftp-adapter serve

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-ftp-adapter:riverhog-ftp-adapter-riverhog-ftp-adapter-serve:38b235acaa -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-ftp-adapter` |
| Interface | `cli` |
| Family | `riverhog-ftp-adapter serve` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/cli/riverhog-ftp-adapter/commands/serve/name`
- `/external_contract/cli/riverhog-ftp-adapter/commands/serve/parameters`

## Effective policies

- `compatibility/cli/v1`

## Executable sources and proof

- `cli:riverhog-ftp-adapter` — `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/app.py::<module>`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make dist-smoke`
- Proof: `make operation-qualification`

## Contract summary

- Parser name: `riverhog-ftp-adapter serve`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `` | _StoreAction | no |  | --host |
| `` | _StoreAction | no | int | --port |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/riverhog-ftp-adapter/commands/serve/name`

<!-- exact-contract-value: 2ca0d748a201f8644f74e0c9571e9ca013591256b58b5fd1efa0f35af93b9aab -->

```json
"riverhog-ftp-adapter serve"
```

### `/external_contract/cli/riverhog-ftp-adapter/commands/serve/parameters`

<!-- exact-contract-value: 2aadd7953ad93b810b29e6f222682b668f490090359802191f32bd6331f8b9d1 -->

```json
[
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
    "default": 8082,
    "dest": "port",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--port"
    ],
    "required": false,
    "type": "int"
  }
]
```
