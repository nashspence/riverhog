# a-riverhog-opentimestamps-witness

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-opentimestamps-witness:a-riverhog-opentimestamps-witness:bbf8b9f567 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-opentimestamps-witness](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-040fa33a82"></a>Parser name: `a-riverhog-opentimestamps-witness`
- <a id="s-f678ee7bdd"></a>Subcommand selection: required.
- <a id="s-49c8384b9c"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-30d400f864"></a>`state`<br>`--state` | required option; 1 value | Path | not recorded |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-f560e6b81f"></a>`help` | <a id="s-a7229286ad"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-7b3e4f2c27"></a>`0` | <a id="s-87cf41bc4d"></a>`"noncontractual-framework-help"` | <a id="s-56ba797e2d"></a>`"empty"` |
| <a id="s-bba83e262c"></a>`version` | <a id="s-d138c49271"></a>`{"kind":"option-present","options":["--version"]}` | <a id="s-fa6cce800c"></a>`0` | <a id="s-895af89d46"></a>`{"distribution":"a-riverhog-opentimestamps-witness","kind":"installed-coordinated-release-version","serialization":"noncontractual"}` | <a id="s-f7c3eafd3e"></a>`"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --state](#s-30d400f864) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-155d63922b"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-ce5d48cc77"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-opentimestamps-witness](../../../evidence/sources/authorities.md#src-26e499502f) — [some-implementations/riverhog/applications/a-riverhog-opentimestamps-witness/src/a\_riverhog\_opentimestamps\_witness/cli.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-opentimestamps-witness/src/a_riverhog_opentimestamps_witness/cli.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-opentimestamps-witness/allow_abbrev`
- `/external_contract/cli/a-riverhog-opentimestamps-witness/name`
- `/external_contract/cli/a-riverhog-opentimestamps-witness/parameters`
- `/external_contract/cli/a-riverhog-opentimestamps-witness/subcommand_required`
- `/external_contract/cli/a-riverhog-opentimestamps-witness/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-opentimestamps-witness/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-opentimestamps-witness/name`

<!-- exact-contract-value: 652525cf7d50c1698da736f4635c1649a675146184fc82ed72664c12a39188b5 -->

```json
"a-riverhog-opentimestamps-witness"
```

### `/external_contract/cli/a-riverhog-opentimestamps-witness/parameters`

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

### `/external_contract/cli/a-riverhog-opentimestamps-witness/subcommand_required`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-opentimestamps-witness/terminating_controls`

<!-- exact-contract-value: aa55c210bef8ec18a4f9d1ade98e4ce132943c1a6696e0490e12f16048379a62 -->

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
      "distribution": "a-riverhog-opentimestamps-witness",
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
