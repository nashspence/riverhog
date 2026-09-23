# a-riverhog-ftp-spool

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-ftp-spool:a-riverhog-ftp-spool:7ea1739328 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-5675c9b6fb"></a>Parser name: `a-riverhog-ftp-spool`
- <a id="s-5bb5620677"></a>Subcommand selection: optional.
- <a id="s-7ad2aa50fb"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-ce995b3850"></a>`config`<br>`--config` | optional option; 1 value | Path | not recorded |
| <a id="s-f639cbccd2"></a>`base_url`<br>`--base-url` | optional option; 1 value | not recorded | not recorded |
| <a id="s-0748fae603"></a>`token`<br>`--token` | optional option; 1 value | not recorded | not recorded |
| <a id="s-c919c305bb"></a>`allow_insecure_http`<br>`--allow-insecure-http` | optional flag; 0 values | not recorded | not recorded |
| <a id="s-659c96215d"></a>`json`<br>`--json` | optional flag; 0 values | not recorded | `false` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-bbfcacd56d"></a>`help` | <a id="s-fe90de4b43"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-8d4202e04a"></a>`0` | <a id="s-7fd049b9b9"></a>`"noncontractual-framework-help"` | <a id="s-82144ac0c0"></a>`"empty"` |
| <a id="s-4a66428483"></a>`version` | <a id="s-5a949dcf0b"></a>`{"kind":"option-present","options":["--version"]}` | <a id="s-27c938a28f"></a>`0` | <a id="s-baa7c8f16f"></a>`{"distribution":"a-riverhog-ftp-spool","kind":"installed-coordinated-release-version","serialization":"noncontractual"}` | <a id="s-d614c0ade6"></a>`"empty"` |

### Result and failure contract

- <a id="s-8d81ba3986"></a>Result identity: `a-riverhog-ftp-spool-cli-result/root/v1`
- <a id="s-fbf034a4ba"></a>Profile: `a-riverhog-ftp-spool-cli-runtime/v1`
- <a id="s-4d8bad75a5"></a>Structured output: `none`
- <a id="s-2d3e392843"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-50d3288502"></a>`stopped` | <a id="s-97af77c89b"></a>`{"kind":"service-runtime-returned"}` | <a id="s-6525f67051"></a>`0` | <a id="s-cabe758ea3"></a>all: `"no-command-result"` | <a id="s-6efd8d3235"></a>all: `"noncontractual-runtime-log"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-f3968d7641"></a>`usage` | <a id="s-3210eeca22"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-822e68cd1c"></a>`2` | <a id="s-53cc3c9e4c"></a>all: `"empty"` | <a id="s-3106c13de0"></a>all: `"noncontractual-usage-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --config](#s-ce995b3850) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1 |
| [CLI parameter --base-url](#s-f639cbccd2) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1 |
| [CLI parameter --token](#s-0748fae603) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1 |
| [CLI parameter --allow-insecure-http](#s-c919c305bb) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0 |
| [CLI parameter --json](#s-659c96215d) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0 |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-04676086ff"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-e5a62b5f62"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-ftp-spool](../../../evidence/sources/authorities.md#src-4f9bc5584a) — [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/app.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-ftp-spool/allow_abbrev`
- `/external_contract/cli/a-riverhog-ftp-spool/name`
- `/external_contract/cli/a-riverhog-ftp-spool/parameters`
- `/external_contract/cli/a-riverhog-ftp-spool/result_contract`
- `/external_contract/cli/a-riverhog-ftp-spool/subcommand_required`
- `/external_contract/cli/a-riverhog-ftp-spool/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-ftp-spool/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-ftp-spool/name`

<!-- exact-contract-value: dad0a7c651a3726342a74cf71b13d83f48ca68a5c1d5dc4dd8678d8a1d0e0bc3 -->

```json
"a-riverhog-ftp-spool"
```

### `/external_contract/cli/a-riverhog-ftp-spool/parameters`

<!-- exact-contract-value: 78188cb732f6353d8ce220140cc2be3caea1e8c57f41d3df9f8924d717a69842 -->

```json
[
  {
    "dest": "config",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--config"
    ],
    "required": false,
    "type": "Path"
  },
  {
    "dest": "base_url",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--base-url"
    ],
    "required": false
  },
  {
    "dest": "token",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--token"
    ],
    "required": false
  },
  {
    "dest": "allow_insecure_http",
    "kind": "_StoreTrueAction",
    "nargs": 0,
    "options": [
      "--allow-insecure-http"
    ],
    "required": false
  },
  {
    "default": false,
    "dest": "json",
    "kind": "_StoreTrueAction",
    "nargs": 0,
    "options": [
      "--json"
    ],
    "required": false
  }
]
```

### `/external_contract/cli/a-riverhog-ftp-spool/result_contract`

<!-- exact-contract-value: e8f82b9421bf782fc5cefca146ecf157684243400a441dfc41ca219679e25cc0 -->

```json
{
  "failures": [
    {
      "exit_status": 2,
      "id": "usage",
      "selected_by": {
        "kind": "parser-rejected-invocation"
      },
      "stderr": {
        "all": "noncontractual-usage-diagnostic"
      },
      "stdout": {
        "all": "empty"
      }
    }
  ],
  "human_json_relationship": "not-applicable",
  "identity": "a-riverhog-ftp-spool-cli-result/root/v1",
  "profile_id": "a-riverhog-ftp-spool-cli-runtime/v1",
  "structured_output": "none",
  "success": [
    {
      "exit_status": 0,
      "id": "stopped",
      "selected_by": {
        "kind": "service-runtime-returned"
      },
      "stderr": {
        "all": "noncontractual-runtime-log"
      },
      "stdout": {
        "all": "no-command-result"
      }
    }
  ]
}
```

### `/external_contract/cli/a-riverhog-ftp-spool/subcommand_required`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-ftp-spool/terminating_controls`

<!-- exact-contract-value: 5859cdc10714c19ff7df31b907a74f9a6c29dd62a3167f68ba14a0eaa2477ebf -->

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
      "distribution": "a-riverhog-ftp-spool",
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
