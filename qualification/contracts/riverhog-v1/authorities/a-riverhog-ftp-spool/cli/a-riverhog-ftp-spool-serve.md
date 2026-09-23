# a-riverhog-ftp-spool serve

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-ftp-spool:a-riverhog-ftp-spool-serve:6d9682bf88 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-98d28b9fe4"></a>Parser name: `serve`
- <a id="s-bef0e63623"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-ed3894bd71"></a>`host`<br>`--host` | optional option; 1 value | not recorded | `"127.0.0.1"` |
| <a id="s-33ebf7763a"></a>`port`<br>`--port` | optional option; 1 value | int | `8082` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-d61b64593e"></a>`help` | <a id="s-c319926772"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-840407a51a"></a>`0` | <a id="s-aae64c5a00"></a>`"noncontractual-framework-help"` | <a id="s-63aa863f4b"></a>`"empty"` |

### Result and failure contract

- <a id="s-54e04d84f2"></a>Result identity: `a-riverhog-ftp-spool-cli-result/serve/v1`
- <a id="s-173d668b32"></a>Profile: `a-riverhog-ftp-spool-cli-runtime/v1`
- <a id="s-f99d6da820"></a>Structured output: `none`
- <a id="s-e9e4b44df1"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-4d9393dc52"></a>`stopped` | <a id="s-95e9460e1c"></a>`{"kind":"service-runtime-returned"}` | <a id="s-6f83116651"></a>`0` | <a id="s-91b3edbcec"></a>all: `"no-command-result"` | <a id="s-f2de209f5e"></a>all: `"noncontractual-runtime-log"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-6dac098129"></a>`usage` | <a id="s-cd021ae24f"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-f99a739dfa"></a>`2` | <a id="s-4ec09f5d02"></a>all: `"empty"` | <a id="s-f9257f7457"></a>all: `"noncontractual-usage-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --host](#s-ed3894bd71) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --port](#s-33ebf7763a) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-90509ac9d0"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-2a682abfe1"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-ftp-spool](../../../evidence/sources/authorities.md#src-4f9bc5584a) — [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/app.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-ftp-spool/commands/serve/allow_abbrev`
- `/external_contract/cli/a-riverhog-ftp-spool/commands/serve/name`
- `/external_contract/cli/a-riverhog-ftp-spool/commands/serve/parameters`
- `/external_contract/cli/a-riverhog-ftp-spool/commands/serve/result_contract`
- `/external_contract/cli/a-riverhog-ftp-spool/commands/serve/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-ftp-spool/commands/serve/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-ftp-spool/commands/serve/name`

<!-- exact-contract-value: f9cb5b4cc5c002946a5ca44426d4db8fbdf0ba02d0850cccb953338686f2de7c -->

```json
"serve"
```

### `/external_contract/cli/a-riverhog-ftp-spool/commands/serve/parameters`

<!-- exact-contract-value: 2aadd7953ad93b810b29e6f222682b668f490090359802191f32bd6331f8b9d1 -->

```json
[
  {
    "default": "127.0.0.1",
    "dest": "host",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--host"
    ],
    "required": false
  },
  {
    "default": 8082,
    "dest": "port",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--port"
    ],
    "required": false,
    "type": "int"
  }
]
```

### `/external_contract/cli/a-riverhog-ftp-spool/commands/serve/result_contract`

<!-- exact-contract-value: 4589f644c0314b3094dd137406c7072429510083eccdff4c2dea4f0247e4915f -->

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
  "identity": "a-riverhog-ftp-spool-cli-result/serve/v1",
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

### `/external_contract/cli/a-riverhog-ftp-spool/commands/serve/terminating_controls`

<!-- exact-contract-value: 46c96c22d2ed8a51da57bba3ac0f2269bb35f98c5fd6dc3e3ba67e60f772ad72 -->

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
  }
]
```

</details>
