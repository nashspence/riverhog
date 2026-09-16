# riverhog-storage-adapter-backblaze

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-storage-adapter-backblaze:riverhog-storage-adapter-backblaze:d2365d8d15 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-backblaze](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-d408790744"></a>Parser name: `riverhog-storage-adapter-backblaze`
- <a id="s-e220dab263"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-f828071054"></a>`host`<br>`--host` | optional option; 1 value | not recorded | `"127.0.0.1"` |
| <a id="s-340d0ae8e0"></a>`port`<br>`--port` | optional option; 1 value | int | `8080` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-d527ef3d83"></a>`help` | <a id="s-ddd318a3a1"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-16e74a0638"></a>`0` | <a id="s-2fa11f5214"></a>`"noncontractual-framework-help"` | <a id="s-ea0cbb90ea"></a>`"empty"` |
| <a id="s-90813e4268"></a>`version` | <a id="s-eae6e65cd6"></a>`{"kind":"option-present","options":["--version"]}` | <a id="s-773a9b848c"></a>`0` | <a id="s-7610d91819"></a>`{"distribution":"riverhog-storage-adapter-backblaze","kind":"installed-coordinated-release-version","serialization":"noncontractual"}` | <a id="s-1f2a9b252c"></a>`"empty"` |

### Result and failure contract

- <a id="s-3a3caabfb4"></a>Result identity: `riverhog-storage-adapter-backblaze-cli-result/root/v1`
- <a id="s-6e3dadf64e"></a>Profile: `riverhog-storage-adapter-backblaze-cli-runtime/v1`
- <a id="s-3267e2e7ac"></a>Structured output: `none`
- <a id="s-bc95f50c09"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-c0c8bd1ba6"></a>`stopped` | <a id="s-47f91ebaff"></a>`{"kind":"service-runtime-returned"}` | <a id="s-214e9db294"></a>`0` | <a id="s-3e4e9ab14c"></a>all: `"no-command-result"` | <a id="s-ac0ae00d80"></a>all: `"noncontractual-runtime-log"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-f221664aad"></a>`usage` | <a id="s-344a0f3262"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-a85bfb4fc9"></a>`2` | <a id="s-7db421a3eb"></a>all: `"empty"` | <a id="s-7d5847e4a4"></a>all: `"noncontractual-usage-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --host](#s-f828071054) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --port](#s-340d0ae8e0) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-c3c3374a48"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-2ac75a81ea"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:riverhog-storage-adapter-backblaze](../../../evidence/sources.md#src-f277cad16d) — `reference/riverhog/storage/backblaze/src/riverhog_storage_adapter_backblaze/app.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/riverhog-storage-adapter-backblaze/allow_abbrev`
- `/external_contract/cli/riverhog-storage-adapter-backblaze/name`
- `/external_contract/cli/riverhog-storage-adapter-backblaze/parameters`
- `/external_contract/cli/riverhog-storage-adapter-backblaze/result_contract`
- `/external_contract/cli/riverhog-storage-adapter-backblaze/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/riverhog-storage-adapter-backblaze/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/riverhog-storage-adapter-backblaze/name`

<!-- exact-contract-value: 31aac68b803b2733099364b56d483b9337c73d5df966a1967f1855651c9723bc -->

```json
"riverhog-storage-adapter-backblaze"
```

### `/external_contract/cli/riverhog-storage-adapter-backblaze/parameters`

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

### `/external_contract/cli/riverhog-storage-adapter-backblaze/result_contract`

<!-- exact-contract-value: a41cb2a4b47691e39f0dda11ac83ba8d53e7930c135f95295bfaa21e436995d1 -->

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
  "identity": "riverhog-storage-adapter-backblaze-cli-result/root/v1",
  "profile_id": "riverhog-storage-adapter-backblaze-cli-runtime/v1",
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

### `/external_contract/cli/riverhog-storage-adapter-backblaze/terminating_controls`

<!-- exact-contract-value: f35825cfe528b0c731f6937bf23d19e248846f1a8454a63274f1fc5835b06a11 -->

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
      "distribution": "riverhog-storage-adapter-backblaze",
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
