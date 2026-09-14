# riverhog-recover

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-recover:riverhog-recover:f71eceba61 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-recover](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-c9dbdcd915"></a>Parser name: `riverhog-recover`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-bd0c880f02"></a>`version` | _VersionAction | no |  | --version |
| <a id="s-3af69a838a"></a>`archive` | _StoreAction | yes | Path |  |
| <a id="s-15d3edeac1"></a>`output` | _StoreAction | no | Path |  |
| <a id="s-ae476bc569"></a>`description_only` | _StoreTrueAction | no |  | --description-only |
| <a id="s-27589f8407"></a>`tags_only` | _StoreTrueAction | no |  | --tags-only |
| <a id="s-bc196a2661"></a>`passphrases_file` | _StoreAction | no | Path | --passphrases-file |
| <a id="s-95cdb2042d"></a>`age_command` | _StoreAction | no |  | --age-command |

### Result and failure contract

- <a id="s-53870ed077"></a>Result identity: `riverhog-recover-cli-result/root/v1`
- <a id="s-26c4bed674"></a>Profile: `riverhog-recover-cli/v1`
- <a id="s-6073c20d9b"></a>Structured output: `mode-specific`
- <a id="s-2368ff4334"></a>Human/JSON relationship: `mode-specific-results`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-e6ffcd91df"></a>`archive-recovered` | <a id="s-cc80a4f4a0"></a>`0` | <a id="s-82103433e9"></a>`{"human":"noncontractual-recovery-summary"}` | <a id="s-1b310db431"></a>`{"all":"empty"}` |
| <a id="s-1cd6f69133"></a>`description-recovered` | <a id="s-e8bbbcc49c"></a>`0` | <a id="s-bdf9e5feff"></a>`{"json":"riverhog-collection-description/v1-or-null"}` | <a id="s-018043ff9a"></a>`{"all":"empty"}` |
| <a id="s-b4a66c971c"></a>`tags-recovered` | <a id="s-70c0bd4c11"></a>`0` | <a id="s-d382d2c367"></a>`{"json":"riverhog-recovered-collection-tags/v1-json-sequence"}` | <a id="s-cb2e774c6f"></a>`{"all":"empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-f02faf7dad"></a>`usage` | <a id="s-33edb1b1ec"></a>`2` | <a id="s-19f9334bbd"></a>`{"all":"empty"}` | <a id="s-2dfd45a472"></a>`{"all":"noncontractual-usage-diagnostic"}` |
| <a id="s-4b8e5acd9d"></a>`recovery` | <a id="s-3c2553c1f0"></a>`1` | <a id="s-664d5b2d56"></a>`{"all":"empty"}` | <a id="s-8ca9ad09a8"></a>`{"all":"riverhog-recover-diagnostic/v1"}` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --version](#s-bd0c880f02) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --description-only](#s-ae476bc569) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --tags-only](#s-27589f8407) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-4bcf7aaad1"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-5c7ad8b3c5"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:riverhog-recover](../../../evidence/sources.md#src-375119d633) — `reference/riverhog/recovery/src/riverhog_recover/cli.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/riverhog-recover/name`
- `/external_contract/cli/riverhog-recover/parameters`
- `/external_contract/cli/riverhog-recover/result_contract`

### Exact owned JSON

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

### `/external_contract/cli/riverhog-recover/result_contract`

<!-- exact-contract-value: 6a69a98e727be36bab32e649cea5d6206309c9d6b8691e8bb5bc6c28a23949f2 -->

```json
{
  "failures": [
    {
      "exit_status": 2,
      "id": "usage",
      "stderr": {
        "all": "noncontractual-usage-diagnostic"
      },
      "stdout": {
        "all": "empty"
      }
    },
    {
      "exit_status": 1,
      "id": "recovery",
      "stderr": {
        "all": "riverhog-recover-diagnostic/v1"
      },
      "stdout": {
        "all": "empty"
      }
    }
  ],
  "human_json_relationship": "mode-specific-results",
  "identity": "riverhog-recover-cli-result/root/v1",
  "profile_id": "riverhog-recover-cli/v1",
  "structured_output": "mode-specific",
  "success": [
    {
      "exit_status": 0,
      "id": "archive-recovered",
      "stderr": {
        "all": "empty"
      },
      "stdout": {
        "human": "noncontractual-recovery-summary"
      }
    },
    {
      "exit_status": 0,
      "id": "description-recovered",
      "stderr": {
        "all": "empty"
      },
      "stdout": {
        "json": "riverhog-collection-description/v1-or-null"
      }
    },
    {
      "exit_status": 0,
      "id": "tags-recovered",
      "stderr": {
        "all": "empty"
      },
      "stdout": {
        "json": "riverhog-recovered-collection-tags/v1-json-sequence"
      }
    }
  ]
}
```
