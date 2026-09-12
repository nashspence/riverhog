# riverhog-recover

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-recover:riverhog-recover:e47dfeaf6b -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-recover` |
| Interface | `cli` |
| Family | `root` |
| Contract elements | 1 |
| Extent decisions | 3 |

## Machine authority

- `/external_contract/cli/riverhog-recover/name`
- `/external_contract/cli/riverhog-recover/parameters`

## Effective policies

- `compatibility/cli/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `cli:riverhog-recover` — `reference/riverhog/recovery/src/riverhog_recover/cli.py::<module>`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make dist-smoke`
- Proof: `make operation-qualification`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | values-per-occurrence | `fixed` | maximum=0, minimum=0, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=0, minimum=0, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=0, minimum=0, reason=fixed-command-argument-arity |

## Contract summary

- Parser name: `riverhog-recover`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `` | _VersionAction | no |  | --version |
| `` | _StoreAction | yes | Path |  |
| `` | _StoreAction | no | Path |  |
| `` | _StoreTrueAction | no |  | --description-only |
| `` | _StoreTrueAction | no |  | --tags-only |
| `` | _StoreAction | no | Path | --passphrases-file |
| `` | _StoreAction | no |  | --age-command |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/riverhog-recover/name`

<!-- exact-contract-value: 62aebf155460f3da3f1210d92abf5b71ae96fb125e9f2c6f92e1ef7cfe99a7d1 -->

```json
"riverhog-recover"
```

### `/external_contract/cli/riverhog-recover/parameters`

<!-- exact-contract-value: 1ddb9b9907f6820c593c5aa98f25e7a54c241550959f0d55be651897d4758548 -->

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
    "dest": "archive",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [],
    "required": true,
    "type": "Path"
  },
  {
    "dest": "output",
    "kind": "_StoreAction",
    "nargs": "?",
    "options": [],
    "required": false,
    "type": "Path"
  },
  {
    "default": false,
    "dest": "description_only",
    "kind": "_StoreTrueAction",
    "nargs": 0,
    "options": [
      "--description-only"
    ],
    "required": false
  },
  {
    "default": false,
    "dest": "tags_only",
    "kind": "_StoreTrueAction",
    "nargs": 0,
    "options": [
      "--tags-only"
    ],
    "required": false
  },
  {
    "dest": "passphrases_file",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--passphrases-file"
    ],
    "required": false,
    "type": "Path"
  },
  {
    "default": "age",
    "dest": "age_command",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--age-command"
    ],
    "required": false
  }
]
```
