# a-riverhog-event-relay

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-event-relay:a-riverhog-event-relay:80e87707e9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-event-relay](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-f67f82a3a6"></a>Parser name: `a-riverhog-event-relay`
- <a id="s-5347711a9b"></a>Subcommand selection: optional.
- <a id="s-8fcd1075f3"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-891df1d8b0"></a>`config`<br>`--config` | required option; 1 value | Path | not recorded |
| <a id="s-ac479b912f"></a>`check`<br>`--check` | optional flag; 0 values | not recorded | `false` |
| <a id="s-6663899f27"></a>`once`<br>`--once` | optional flag; 0 values | not recorded | `false` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-281445e7a7"></a>`help` | <a id="s-2de6de3097"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-8b4de45f88"></a>`0` | <a id="s-2a364c6b2e"></a>`"noncontractual-framework-help"` | <a id="s-eae632a985"></a>`"empty"` |
| <a id="s-ea9c19d311"></a>`version` | <a id="s-4ee7d3ee00"></a>`{"kind":"option-present","options":["--version"]}` | <a id="s-f2f7bc67bb"></a>`0` | <a id="s-1863463384"></a>`{"distribution":"a-riverhog-event-relay","kind":"installed-coordinated-release-version","serialization":"noncontractual"}` | <a id="s-a39a97da0f"></a>`"empty"` |

### Result and failure contract

- <a id="s-a591097f3c"></a>Result identity: `a-riverhog-event-relay-cli-result/root/v1`
- <a id="s-c47d5eec91"></a>Profile: `a-riverhog-event-relay-cli-relay-runtime/v1`
- <a id="s-c894545b96"></a>Structured output: `mode-specific`
- <a id="s-abcc74f693"></a>Human/JSON relationship: `mode-specific-results`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-30e86df7b1"></a>`configuration-check` | <a id="s-01bab75029"></a>`{"kind":"option-equals","parameter":"check","value":true}` | <a id="s-d83012183a"></a>`0` | <a id="s-bb465fb5a4"></a>all: `"a-riverhog-event-relay-configuration-summary/v1"` | <a id="s-25dfb69ea4"></a>all: `"noncontractual-runtime-log-or-empty"` |
| <a id="s-804093760b"></a>`relay-completed` | <a id="s-59b01a0641"></a>`{"kind":"option-equals","parameter":"check","value":false}` | <a id="s-f5450d4827"></a>`0` | <a id="s-f5fb56011e"></a>all: `"no-command-result"` | <a id="s-49f290cd97"></a>all: `"noncontractual-runtime-log-or-empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-1316923743"></a>`usage` | <a id="s-a0fced932c"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-d754f04d8a"></a>`2` | <a id="s-043104483a"></a>all: `"empty"` | <a id="s-122cc5efbe"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-a33d40efb3"></a>`relay-pass-failed` | <a id="s-fc7ea0488a"></a>`{"kind":"relay-pass-reported-failures"}` | <a id="s-2025037b4b"></a>`1` | <a id="s-6e9d6b81a6"></a>all: `"no-command-result"` | <a id="s-c6b13962b4"></a>all: `"noncontractual-runtime-log"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --config](#s-891df1d8b0) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1 |
| [CLI parameter --check](#s-ac479b912f) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0 |
| [CLI parameter --once](#s-6663899f27) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0 |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-f2c5fcb1bc"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-4276941b8f"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-event-relay](../../../evidence/sources/authorities.md#src-79ff0dba62) — [some-implementations/riverhog/applications/a-riverhog-event-relay/src/a\_riverhog\_event\_relay/cli.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-event-relay/src/a_riverhog_event_relay/cli.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-event-relay/allow_abbrev`
- `/external_contract/cli/a-riverhog-event-relay/name`
- `/external_contract/cli/a-riverhog-event-relay/parameters`
- `/external_contract/cli/a-riverhog-event-relay/result_contract`
- `/external_contract/cli/a-riverhog-event-relay/subcommand_required`
- `/external_contract/cli/a-riverhog-event-relay/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-event-relay/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-event-relay/name`

<!-- exact-contract-value: 337256b4fc27d5e3924864b2f3aed69bd6cd4b7bce9390098d3951e1477a2250 -->

```json
"a-riverhog-event-relay"
```

### `/external_contract/cli/a-riverhog-event-relay/parameters`

<!-- exact-contract-value: b096906e4d3ad7dfc114a853c3f02bf4cc135bc3f5843f13cfd8572456089113 -->

```json
[
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

### `/external_contract/cli/a-riverhog-event-relay/result_contract`

<!-- exact-contract-value: 300abde66ffc7fc4e3987273fb5e71a32afe7ddfaaff87a7e3991c0d3eee7f28 -->

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
    },
    {
      "exit_status": 1,
      "id": "relay-pass-failed",
      "selected_by": {
        "kind": "relay-pass-reported-failures"
      },
      "stderr": {
        "all": "noncontractual-runtime-log"
      },
      "stdout": {
        "all": "no-command-result"
      }
    }
  ],
  "human_json_relationship": "mode-specific-results",
  "identity": "a-riverhog-event-relay-cli-result/root/v1",
  "profile_id": "a-riverhog-event-relay-cli-relay-runtime/v1",
  "structured_output": "mode-specific",
  "success": [
    {
      "exit_status": 0,
      "id": "configuration-check",
      "selected_by": {
        "kind": "option-equals",
        "parameter": "check",
        "value": true
      },
      "stderr": {
        "all": "noncontractual-runtime-log-or-empty"
      },
      "stdout": {
        "all": "a-riverhog-event-relay-configuration-summary/v1"
      }
    },
    {
      "exit_status": 0,
      "id": "relay-completed",
      "selected_by": {
        "kind": "option-equals",
        "parameter": "check",
        "value": false
      },
      "stderr": {
        "all": "noncontractual-runtime-log-or-empty"
      },
      "stdout": {
        "all": "no-command-result"
      }
    }
  ]
}
```

### `/external_contract/cli/a-riverhog-event-relay/subcommand_required`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-event-relay/terminating_controls`

<!-- exact-contract-value: 79f7736a91a1056067158c41b7a1d90cad63f8438b35c54e752c4aad274dd410 -->

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
      "distribution": "a-riverhog-event-relay",
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
