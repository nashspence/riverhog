# a-riverhog-minisign-witness

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-minisign-witness:a-riverhog-minisign-witness:8b9004f05f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-minisign-witness](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-0fd90e391b"></a>Parser name: `a-riverhog-minisign-witness`
- <a id="s-3e4ae0b634"></a>Subcommand selection: required.
- <a id="s-e2e9de7c77"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-513db2cdba"></a>`state`<br>`--state` | required option; 1 value | Path | not recorded |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-eefba77b94"></a>`help` | <a id="s-aa0140e3cb"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-f471e35b7c"></a>`0` | <a id="s-094da2f92a"></a>`"noncontractual-framework-help"` | <a id="s-ac63c6e85b"></a>`"empty"` |
| <a id="s-4fa1881b69"></a>`version` | <a id="s-db26f5a819"></a>`{"kind":"option-present","options":["--version"]}` | <a id="s-ebdebbdbb8"></a>`0` | <a id="s-44019c7e4a"></a>`{"distribution":"a-riverhog-minisign-witness","kind":"installed-coordinated-release-version","serialization":"noncontractual"}` | <a id="s-01918ce5cd"></a>`"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --state](#s-513db2cdba) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-55a832cb95"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-4be7fb49ac"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-minisign-witness](../../../evidence/sources/authorities.md#src-ab8103c6de) — [some-implementations/riverhog/applications/a-riverhog-minisign-witness/src/a\_riverhog\_minisign\_witness/cli.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-minisign-witness/src/a_riverhog_minisign_witness/cli.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-minisign-witness/allow_abbrev`
- `/external_contract/cli/a-riverhog-minisign-witness/name`
- `/external_contract/cli/a-riverhog-minisign-witness/parameters`
- `/external_contract/cli/a-riverhog-minisign-witness/subcommand_required`
- `/external_contract/cli/a-riverhog-minisign-witness/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-minisign-witness/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-minisign-witness/name`

<!-- exact-contract-value: e0e56be41cc086f192911e756856fca6a8ef627c90f60d3c462e405108bd0afb -->

```json
"a-riverhog-minisign-witness"
```

### `/external_contract/cli/a-riverhog-minisign-witness/parameters`

<!-- exact-contract-value: 228c1c3e9cb663245ebee7d2223c56b0cd7d32b84da5cb739c2e7997acff36e4 -->

```json
[
  {
    "dest": "state",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--state"
    ],
    "required": true,
    "type": "Path"
  }
]
```

### `/external_contract/cli/a-riverhog-minisign-witness/subcommand_required`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-minisign-witness/terminating_controls`

<!-- exact-contract-value: bd0050b4016f2702f62a219f995dd7757396e28b7b76d27f96e8481ca1c41d3a -->

```json
[
  {
    "exit_status": 0,
    "id": "help",
    "stderr": "empty",
    "stdout": "noncontractual-framework-help",
    "trigger": {
      "kind": "option-present",
      "options": [
        "-h",
        "--help"
      ]
    }
  },
  {
    "exit_status": 0,
    "id": "version",
    "stderr": "empty",
    "stdout": {
      "distribution": "a-riverhog-minisign-witness",
      "kind": "installed-coordinated-release-version",
      "serialization": "noncontractual"
    },
    "trigger": {
      "kind": "option-present",
      "options": [
        "--version"
      ]
    }
  }
]
```

</details>
