# riverhog-recover

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-recover:riverhog-recover:e47dfeaf6b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-recover](../index.md) |
| Interface | [cli](index.md) |
| Family | [root](index.md#f-67c1311009ab) |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

- <a id="s-c9dbdcd91594"></a>Parser name: `riverhog-recover`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-bd0c880f02b6"></a>`` | _VersionAction | no |  | --version |
| <a id="s-3af69a838ada"></a>`` | _StoreAction | yes | Path |  |
| <a id="s-15d3edeac135"></a>`` | _StoreAction | no | Path |  |
| <a id="s-ae476bc56983"></a>`` | _StoreTrueAction | no |  | --description-only |
| <a id="s-27589f8407d1"></a>`` | _StoreTrueAction | no |  | --tags-only |
| <a id="s-bc196a266165"></a>`` | _StoreAction | no | Path | --passphrases-file |
| <a id="s-95cdb2042d42"></a>`` | _StoreAction | no |  | --age-command |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --version](#s-bd0c880f02b6) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --description-only](#s-ae476bc56983) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --tags-only](#s-27589f8407d1) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-aa263439cde7"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-ecc54738d336"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [cli:riverhog-recover](../../../evidence/sources.md#src-375119d633c0) — `reference/riverhog/recovery/src/riverhog_recover/cli.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/riverhog-recover/name`
- `/external_contract/cli/riverhog-recover/parameters`

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
