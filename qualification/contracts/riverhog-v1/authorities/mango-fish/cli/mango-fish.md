# mango-fish

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:mango-fish:mango-fish:afd5034e40 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [mango-fish](../index.md) |
| Interface | [cli](index.md) |
| Family | [root](index.md#f-e95e81e4dd95) |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

- <a id="s-8e837eac2f2f"></a>Parser name: `mango-fish`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-1368f192cf16"></a>`` | _VersionAction | no |  | --version |
| <a id="s-b3bbac485dbf"></a>`` | _StoreAction | yes | Path | --config |
| <a id="s-55e6dc25a495"></a>`` | _StoreTrueAction | no |  | --check |
| <a id="s-04b810bfdff9"></a>`` | _StoreTrueAction | no |  | --once |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --version](#s-1368f192cf16) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --check](#s-55e6dc25a495) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --once](#s-04b810bfdff9) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-6b1303b6ff11"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-03e056b27932"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [cli:mango-fish](../../../evidence/sources.md#src-3dcd5eedf2a0) — `reference/riverhog/applications/mango-fish/src/mango_fish/cli.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/mango-fish/name`
- `/external_contract/cli/mango-fish/parameters`

### Exact owned JSON

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
