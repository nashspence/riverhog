# riverhog-ftp-adapter riverhog-ftp-adapter flush

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-ftp-adapter:riverhog-ftp-adapter-riverhog-ftp-adapter-flush:f46ca862d8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [cli](index.md) |
| Family | [riverhog-ftp-adapter flush](index.md#f-52f20f17cd01) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- <a id="s-2ec785212440"></a>Parser name: `riverhog-ftp-adapter flush`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-206a3aa72105"></a>`` | _StoreAction | yes |  |  |

## Governing policies

- <a id="pa-6b5ea447df07"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [cli:riverhog-ftp-adapter](../../../evidence/sources.md#src-303f765bca1e) — `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/app.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/riverhog-ftp-adapter/commands/flush/name`
- `/external_contract/cli/riverhog-ftp-adapter/commands/flush/parameters`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/riverhog-ftp-adapter/commands/flush/name`

<!-- exact-contract-value: 4f6db9cdb3fe2fe75e4e35e7b382b5da6b4c1e78dbd7c47fefba1c40fd71be40 -->

```json
"riverhog-ftp-adapter flush"
```

### `/external_contract/cli/riverhog-ftp-adapter/commands/flush/parameters`

<!-- exact-contract-value: 92534cadceaf71260a6bb9cc94a449a3c15a506af0b41425543fdbd26c2c1d32 -->

```json
[
  {
    "dest": "source",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [],
    "required": true
  }
]
```
