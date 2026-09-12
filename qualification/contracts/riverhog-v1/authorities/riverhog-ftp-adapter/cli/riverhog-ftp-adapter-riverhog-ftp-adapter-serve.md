# riverhog-ftp-adapter riverhog-ftp-adapter serve

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-ftp-adapter:riverhog-ftp-adapter-riverhog-ftp-adapter-serve:38b235acaa -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [cli](index.md) |
| Family | [riverhog-ftp-adapter serve](index.md#f-6d617fe38b91) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- <a id="s-cf31bb6074ec"></a>Parser name: `riverhog-ftp-adapter serve`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-8ae0b92557a6"></a>`` | _StoreAction | no |  | --host |
| <a id="s-cb5e793bc3a3"></a>`` | _StoreAction | no | int | --port |

## Governing policies

- <a id="pa-0dc4d7b1e5f5"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [cli:riverhog-ftp-adapter](../../../evidence/sources.md#src-303f765bca1e) — `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/app.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/riverhog-ftp-adapter/commands/serve/name`
- `/external_contract/cli/riverhog-ftp-adapter/commands/serve/parameters`

### Exact owned JSON

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
