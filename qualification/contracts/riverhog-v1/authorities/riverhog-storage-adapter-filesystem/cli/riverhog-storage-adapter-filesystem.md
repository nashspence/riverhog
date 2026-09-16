# riverhog-storage-adapter-filesystem

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-storage-adapter-filesystem:riverhog-storage-adapter-filesystem:ae17437fb6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-filesystem](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-2648424075"></a>Parser name: `riverhog-storage-adapter-filesystem`

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-81389da66d"></a>`host`<br>`--host` | optional option; 1 value | not recorded | `"127.0.0.1"` |
| <a id="s-e998ac9369"></a>`port`<br>`--port` | optional option; 1 value | int | `8080` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-97ffe2d797"></a>`help` | <a id="s-1cfcb0d9bd"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-080cef0c88"></a>`0` | <a id="s-746d1793f3"></a>`"noncontractual-framework-help"` | <a id="s-de9f7379a6"></a>`"empty"` |
| <a id="s-8c80be2637"></a>`version` | <a id="s-6d2c50c42f"></a>`{"kind":"option-present","options":["--version"]}` | <a id="s-8b442666d3"></a>`0` | <a id="s-5f78cca0e1"></a>`{"distribution":"riverhog-storage-adapter-filesystem","kind":"installed-coordinated-release-version","serialization":"noncontractual"}` | <a id="s-59a9b07b1a"></a>`"empty"` |

### Result and failure contract

- <a id="s-c6f3f6097b"></a>Result identity: `riverhog-storage-adapter-filesystem-cli-result/root/v1`
- <a id="s-6f945f4bc4"></a>Profile: `riverhog-storage-adapter-filesystem-cli-runtime/v1`
- <a id="s-48c29d946a"></a>Structured output: `none`
- <a id="s-76f3a8964c"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-b535643730"></a>`stopped` | <a id="s-c0ec901854"></a>`{"kind":"service-runtime-returned"}` | <a id="s-60b675e3f7"></a>`0` | <a id="s-2fa96ca046"></a>all: `no-command-result` | <a id="s-53a70ed72e"></a>all: `noncontractual-runtime-log` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-0976338f20"></a>`usage` | <a id="s-afdc2e9860"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-2e318f88ed"></a>`2` | <a id="s-037a372f7e"></a>all: `empty` | <a id="s-2a949bad3c"></a>all: `noncontractual-usage-diagnostic` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --host](#s-81389da66d) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --port](#s-e998ac9369) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-1a193ffb19"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-c23c341e6d"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:riverhog-storage-adapter-filesystem](../../../evidence/sources.md#src-ac98690b10) — `reference/riverhog/storage/filesystem/src/riverhog_storage_adapter_filesystem/app.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/riverhog-storage-adapter-filesystem/name`
- `/external_contract/cli/riverhog-storage-adapter-filesystem/parameters`
- `/external_contract/cli/riverhog-storage-adapter-filesystem/result_contract`
- `/external_contract/cli/riverhog-storage-adapter-filesystem/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/riverhog-storage-adapter-filesystem/name`

<!-- exact-contract-value: 2e81023ef8b7c01ec767c28fa414a6f825686c28b712622688e2d5df31b6fa5f -->

```json
"riverhog-storage-adapter-filesystem"
```

### `/external_contract/cli/riverhog-storage-adapter-filesystem/parameters`

<!-- exact-contract-value: ecc6f99dbe14513025a57c9b575b12b7e3c3add71a319df647c17cd2f15bcd28 -->

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
  }
]
```

### `/external_contract/cli/riverhog-storage-adapter-filesystem/result_contract`

<!-- exact-contract-value: 4c53e28dbc8d1429d9045f38387008bbc02ad7280514fc5b052070ea04fbf73b -->

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
  "identity": "riverhog-storage-adapter-filesystem-cli-result/root/v1",
  "profile_id": "riverhog-storage-adapter-filesystem-cli-runtime/v1",
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

### `/external_contract/cli/riverhog-storage-adapter-filesystem/terminating_controls`

<!-- exact-contract-value: 57a2df765e96df1f97d812b5ceab4cf9a31bc36c0c29eaa090d6299af0ea04d0 -->

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
      "distribution": "riverhog-storage-adapter-filesystem",
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
