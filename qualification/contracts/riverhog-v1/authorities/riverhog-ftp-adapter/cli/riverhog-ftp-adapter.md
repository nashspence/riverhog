# riverhog-ftp-adapter

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-ftp-adapter:riverhog-ftp-adapter:43f100f77c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [cli](index.md) |
| Family | [root](index.md#f-925a0d0b0d) |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

- <a id="s-3b77079260"></a>Parser name: `riverhog-ftp-adapter`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-90b07e9fd2"></a>`` | _VersionAction | no |  | --version |
| <a id="s-f757f099cb"></a>`` | _StoreAction | no | Path | --config |
| <a id="s-4088337b61"></a>`` | _StoreAction | no |  | --base-url |
| <a id="s-51acb7a74a"></a>`` | _StoreAction | no |  | --token |
| <a id="s-a4af6d5982"></a>`` | _StoreTrueAction | no |  | --allow-insecure-http |
| <a id="s-7d46672345"></a>`` | _StoreTrueAction | no |  | --json |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --version](#s-90b07e9fd2) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --allow-insecure-http](#s-a4af6d5982) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --json](#s-7d46672345) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-8f2b79cfcc"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-119ab1a159"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:riverhog-ftp-adapter](../../../evidence/sources.md#src-303f765bca) — `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/app.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/riverhog-ftp-adapter/name`
- `/external_contract/cli/riverhog-ftp-adapter/parameters`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/riverhog-ftp-adapter/name`

<!-- exact-contract-value: 7e14ae24d5b05761347a785bd381607ae0f3f00e29d17470741f0a64d8f9e3bc -->

```json
"riverhog-ftp-adapter"
```

### `/external_contract/cli/riverhog-ftp-adapter/parameters`

<!-- exact-contract-value: 5b0240d1d5a69359f4143950fdd586f7ee977d825a9f22f78b4493462265d79a -->

```json
[
  {
    "dest": "version",
    "kind": "_VersionAction",
    "nargs": 0,
    "options": [
      "--version"
    ],
    "required": false
  },
  {
    "dest": "config",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--config"
    ],
    "required": false,
    "type": "Path"
  },
  {
    "dest": "base_url",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--base-url"
    ],
    "required": false
  },
  {
    "dest": "token",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--token"
    ],
    "required": false
  },
  {
    "dest": "allow_insecure_http",
    "kind": "_StoreTrueAction",
    "nargs": 0,
    "options": [
      "--allow-insecure-http"
    ],
    "required": false
  },
  {
    "default": false,
    "dest": "json",
    "kind": "_StoreTrueAction",
    "nargs": 0,
    "options": [
      "--json"
    ],
    "required": false
  }
]
```
