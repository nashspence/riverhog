# stove0

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-stove0-cli:stove0:49f16904d5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-6a82e759aa"></a>Parser name: `stove0`
- <a id="s-9e3acf0583"></a>Subcommand selection: required.
- <a id="s-eb0cf4e165"></a>Extra arguments at this parser: accepted. Subcommand selection and child parsing still apply.
- <a id="s-a4d0b20f73"></a>Options after positional arguments at this parser: left as arguments.
- <a id="s-906050735e"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-791a5ac62a"></a>`base_url`<br>`--base-url` | optional option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-ddcd5174d2"></a>`token`<br>`--token` | optional option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-7dc0e8294b"></a>`json_output`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |
| <a id="s-f62c39ef75"></a>`allow_insecure_http`<br>`--allow-insecure-http`, alternate: `--no-allow-insecure-http` | optional flag; 0 values | boolean | not recorded<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-93d55fec59"></a>`help` | <a id="s-957bcb3b93"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-72b1b57d0a"></a>`0` | <a id="s-b04cefcc77"></a>`"noncontractual-framework-help"` | <a id="s-42dd8a9d9f"></a>`"empty"` |
| <a id="s-077cfb592e"></a>`version` | <a id="s-ff5c02073f"></a>`{"kind":"option-present","options":["--version"]}` | <a id="s-e0e077de9e"></a>`0` | <a id="s-33cce1ef1e"></a>`{"distribution":"a-stove0-cli","kind":"installed-coordinated-release-version","serialization":"noncontractual"}` | <a id="s-0a31e295de"></a>`"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --allow-insecure-http](#s-f62c39ef75) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter --base-url](#s-791a5ac62a) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-7dc0e8294b) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter --token](#s-ddcd5174d2) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-cf8f172231"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-56cef3f197"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources/authorities.md#src-6203ae7d88) — [some-implementations/stove0/application/client/src/a\_stove0\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/stove0/application/client/src/a_stove0_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/stove0/allow_extra_args`
- `/external_contract/cli/stove0/allow_interspersed_args`
- `/external_contract/cli/stove0/ignore_unknown_options`
- `/external_contract/cli/stove0/name`
- `/external_contract/cli/stove0/parameters`
- `/external_contract/cli/stove0/subcommand_required`
- `/external_contract/cli/stove0/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/allow_extra_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0/allow_interspersed_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/name`

<!-- exact-contract-value: 66611cf84ca82d3282e91a365ea2fe842c1dc12632248785bcd8f084f06ea0eb -->

```json
"stove0"
```

### `/external_contract/cli/stove0/parameters`

<!-- exact-contract-value: 8d942a43b26a70a73cf6e5ff2094ee42434de3e07ea6056830159e06b185280b -->

```json
[
  {
    "count": false,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "base_url",
    "nargs": 1,
    "options": [
      "--base-url"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.StringParamType",
      "name": "text"
    }
  },
  {
    "count": false,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "token",
    "nargs": 1,
    "options": [
      "--token"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.StringParamType",
      "name": "text"
    }
  },
  {
    "count": false,
    "default": false,
    "envvar": null,
    "is_flag": true,
    "kind": "TyperOption",
    "multiple": false,
    "name": "json_output",
    "nargs": 1,
    "options": [
      "--json"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.BoolParamType",
      "name": "boolean"
    }
  },
  {
    "count": false,
    "envvar": null,
    "is_flag": true,
    "kind": "TyperOption",
    "multiple": false,
    "name": "allow_insecure_http",
    "nargs": 1,
    "options": [
      "--allow-insecure-http"
    ],
    "required": false,
    "secondary_options": [
      "--no-allow-insecure-http"
    ],
    "type": {
      "class": "typer._click.types.BoolParamType",
      "name": "boolean"
    }
  }
]
```

### `/external_contract/cli/stove0/subcommand_required`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0/terminating_controls`

<!-- exact-contract-value: 3a8f9d3587400efce88b137d0bf9e9b71110ea70304d3dc6baffe1e708940ce5 -->

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
        "--help"
      ]
    }
  },
  {
    "exit_status": 0,
    "id": "version",
    "stderr": "empty",
    "stdout": {
      "distribution": "a-stove0-cli",
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
