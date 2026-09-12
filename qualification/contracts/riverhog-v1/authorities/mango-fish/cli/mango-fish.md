# mango-fish

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:mango-fish:mango-fish:afd5034e40 -->

| Audit field | Value |
|---|---|
| Authority | `mango-fish` |
| Interface | `cli` |
| Family | `root` |
| Contract elements | 1 |
| Extent decisions | 3 |

## Machine authority

- `/external_contract/cli/mango-fish/name`
- `/external_contract/cli/mango-fish/parameters`

## Effective policies

- `compatibility/cli/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `cli:mango-fish` — `reference/riverhog/applications/mango-fish/src/mango_fish/cli.py::<module>`
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

- Parser name: `mango-fish`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `` | _VersionAction | no |  | --version |
| `` | _StoreAction | yes | Path | --config |
| `` | _StoreTrueAction | no |  | --check |
| `` | _StoreTrueAction | no |  | --once |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/mango-fish/name`

<!-- exact-contract-value: 1597bf55bb7889ac7479259c9f4631adf1e2188403282e8d82efa49b1815e160 -->

```json
"mango-fish"
```

### `/external_contract/cli/mango-fish/parameters`

<!-- exact-contract-value: 86e7f99d8c26c4a2af74645430cc84a84863561bbcf8a62fc165a551c7e65335 -->

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
    "required": true,
    "type": "Path"
  },
  {
    "default": false,
    "dest": "check",
    "kind": "_StoreTrueAction",
    "nargs": 0,
    "options": [
      "--check"
    ],
    "required": false
  },
  {
    "default": false,
    "dest": "once",
    "kind": "_StoreTrueAction",
    "nargs": 0,
    "options": [
      "--once"
    ],
    "required": false
  }
]
```
