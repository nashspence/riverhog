# riverhog-ftp-adapter serve

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-ftp-adapter:riverhog-ftp-adapter-serve:cb5ffc24ab -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-cf31bb6074"></a>Parser name: `serve`
- <a id="s-54a6813f76"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-8ae0b92557"></a>`host`<br>`--host` | optional option; 1 value | not recorded | `"127.0.0.1"` |
| <a id="s-cb5e793bc3"></a>`port`<br>`--port` | optional option; 1 value | int | `8082` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-19ad769053"></a>`help` | <a id="s-b9145c6bb6"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-16219f8724"></a>`0` | <a id="s-d9053939bc"></a>`"noncontractual-framework-help"` | <a id="s-41f5c818a9"></a>`"empty"` |

### Result and failure contract

- <a id="s-71daf1cf85"></a>Result identity: `riverhog-ftp-adapter-cli-result/serve/v1`
- <a id="s-79d5ed72fc"></a>Profile: `riverhog-ftp-adapter-cli-runtime/v1`
- <a id="s-79dd6727dd"></a>Structured output: `none`
- <a id="s-29180db335"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-c1f2b3d923"></a>`stopped` | <a id="s-c8a0498fcb"></a>`{"kind":"service-runtime-returned"}` | <a id="s-cbdc2fa32a"></a>`0` | <a id="s-32e5231259"></a>all: `no-command-result` | <a id="s-9fa98217a0"></a>all: `noncontractual-runtime-log` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-2d98dbad82"></a>`usage` | <a id="s-bffe4593cf"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-0ec318e545"></a>`2` | <a id="s-8a0b4eec4b"></a>all: `empty` | <a id="s-95095cb125"></a>all: `noncontractual-usage-diagnostic` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --host](#s-8ae0b92557) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --port](#s-cb5e793bc3) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-fe9b5262be"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-6d08c96b07"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:riverhog-ftp-adapter](../../../evidence/sources.md#src-303f765bca) — `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/app.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/riverhog-ftp-adapter/commands/serve/allow_abbrev`
- `/external_contract/cli/riverhog-ftp-adapter/commands/serve/name`
- `/external_contract/cli/riverhog-ftp-adapter/commands/serve/parameters`
- `/external_contract/cli/riverhog-ftp-adapter/commands/serve/result_contract`
- `/external_contract/cli/riverhog-ftp-adapter/commands/serve/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/riverhog-ftp-adapter/commands/serve/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/riverhog-ftp-adapter/commands/serve/name`

<!-- exact-contract-value: f9cb5b4cc5c002946a5ca44426d4db8fbdf0ba02d0850cccb953338686f2de7c -->

```json
"serve"
```

### `/external_contract/cli/riverhog-ftp-adapter/commands/serve/parameters`

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

### `/external_contract/cli/riverhog-ftp-adapter/commands/serve/result_contract`

<!-- exact-contract-value: a83bbe422a64779274ec2c00c6a31bd30a54b4cf9b8e4bd52df34197b41bd0b3 -->

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
  "identity": "riverhog-ftp-adapter-cli-result/serve/v1",
  "profile_id": "riverhog-ftp-adapter-cli-runtime/v1",
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

### `/external_contract/cli/riverhog-ftp-adapter/commands/serve/terminating_controls`

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
