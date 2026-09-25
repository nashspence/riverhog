# a-riverhog-aws-store

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-aws-store:a-riverhog-aws-store:1679e89a22 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-aws-store](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-f1c73ba79e"></a>Parser name: `a-riverhog-aws-store`
- <a id="s-64c2428f78"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-375b606e4b"></a>`host`<br>`--host` | optional option; 1 value | not recorded | `"127.0.0.1"` |
| <a id="s-c69eac7cfd"></a>`port`<br>`--port` | optional option; 1 value | int | `8080` |
| <a id="s-5cd3ecf03d"></a>`provision_root`<br>`--provision-root` | optional flag; 0 values | not recorded | `false` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-e2091c8f4a"></a>`help` | <a id="s-ed6b2c0c34"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-0ac97a8dd9"></a>`0` | <a id="s-485f313f0e"></a>`"noncontractual-framework-help"` | <a id="s-286842d2a3"></a>`"empty"` |
| <a id="s-2eb34e0208"></a>`version` | <a id="s-3a0401d3f1"></a>`{"kind":"option-present","options":["--version"]}` | <a id="s-2aa6fb0a84"></a>`0` | <a id="s-b78166030c"></a>`{"distribution":"a-riverhog-aws-store","kind":"installed-coordinated-release-version","serialization":"noncontractual"}` | <a id="s-d52ca4661c"></a>`"empty"` |

### Result and failure contract

- <a id="s-d992c8bb9e"></a>Result identity: `a-riverhog-aws-store-cli-result/root/v1`
- <a id="s-9f2b320200"></a>Profile: `a-riverhog-aws-store-cli-runtime/v1`
- <a id="s-e952a2f845"></a>Structured output: `none`
- <a id="s-0c94a387cd"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-b73f50a3e6"></a>`stopped` | <a id="s-0bc632484f"></a>`{"kind":"service-runtime-returned"}` | <a id="s-4dab050ea4"></a>`0` | <a id="s-c94a3f2b1c"></a>all: `"no-command-result"` | <a id="s-24335f4ee0"></a>all: `"noncontractual-runtime-log"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-3c8de52669"></a>`usage` | <a id="s-ca9004c467"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-0fb6d1fa32"></a>`2` | <a id="s-4cd77a2f65"></a>all: `"empty"` | <a id="s-65b3a84d5e"></a>all: `"noncontractual-usage-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --host](#s-375b606e4b) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1 |
| [CLI parameter --port](#s-c69eac7cfd) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1 |
| [CLI parameter --provision-root](#s-5cd3ecf03d) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0 |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-c07b709b89"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-cab529a7cf"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-aws-store](../../../evidence/sources/authorities.md#src-7e144eedcc) — [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-aws-store/allow_abbrev`
- `/external_contract/cli/a-riverhog-aws-store/name`
- `/external_contract/cli/a-riverhog-aws-store/parameters`
- `/external_contract/cli/a-riverhog-aws-store/result_contract`
- `/external_contract/cli/a-riverhog-aws-store/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-aws-store/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-aws-store/name`

<!-- exact-contract-value: 44cc17c60a1a95b838fb6cdb1ac9ac3a0ef228032b8a8ee48760d124c1ff9d1d -->

```json
"a-riverhog-aws-store"
```

### `/external_contract/cli/a-riverhog-aws-store/parameters`

<!-- exact-contract-value: 2e7bfbe785f76e807173a7daf67d956193711ab9464e7a8d9054a8340b54b804 -->

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
    "default": 8080,
    "dest": "port",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--port"
    ],
    "required": false,
    "type": "int"
  },
  {
    "default": false,
    "dest": "provision_root",
    "kind": "_StoreTrueAction",
    "nargs": 0,
    "options": [
      "--provision-root"
    ],
    "required": false
  }
]
```

### `/external_contract/cli/a-riverhog-aws-store/result_contract`

<!-- exact-contract-value: 5fbcee9e14927e9c7f909282bce2fa573158d0f668e7fa526e158e331382035f -->

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
  "identity": "a-riverhog-aws-store-cli-result/root/v1",
  "profile_id": "a-riverhog-aws-store-cli-runtime/v1",
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

### `/external_contract/cli/a-riverhog-aws-store/terminating_controls`

<!-- exact-contract-value: 9d00113a469f98303e20ba948486e6bfbb60aed61edbd89c4d8c47990719098e -->

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
      "distribution": "a-riverhog-aws-store",
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
