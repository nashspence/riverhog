# riverhog-ftp-adapter riverhog-ftp-adapter flush

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-ftp-adapter:riverhog-ftp-adapter-riverhog-ftp-adapter-flush:f46ca862d8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog-ftp-adapter` |
| Interface | `cli` |
| Family | `riverhog-ftp-adapter flush` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- Parser name: `riverhog-ftp-adapter flush`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `` | _StoreAction | yes |  |  |

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
