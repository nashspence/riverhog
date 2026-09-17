# riverhog-storage-adapter-aws

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-storage-adapter-aws:riverhog-storage-adapter-aws:1b8597d3b4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-aws](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-b07406fd0f"></a>Parser name: `riverhog-storage-adapter-aws`
- <a id="s-353207f9a3"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-61217926a3"></a>`host`<br>`--host` | optional option; 1 value | not recorded | `"127.0.0.1"` |
| <a id="s-f51932dda3"></a>`port`<br>`--port` | optional option; 1 value | int | `8080` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-a5ac78aa9e"></a>`help` | <a id="s-d36b2423dc"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-362b80d24c"></a>`0` | <a id="s-138cbbcfb6"></a>`"noncontractual-framework-help"` | <a id="s-4586532424"></a>`"empty"` |
| <a id="s-030ca6ccdc"></a>`version` | <a id="s-3a23d3d2ce"></a>`{"kind":"option-present","options":["--version"]}` | <a id="s-adb3859273"></a>`0` | <a id="s-04634a6369"></a>`{"distribution":"riverhog-storage-adapter-aws","kind":"installed-coordinated-release-version","serialization":"noncontractual"}` | <a id="s-3d8d2e5bc0"></a>`"empty"` |

### Result and failure contract

- <a id="s-2a83ccec0c"></a>Result identity: `riverhog-storage-adapter-aws-cli-result/root/v1`
- <a id="s-454a5c4993"></a>Profile: `riverhog-storage-adapter-aws-cli-runtime/v1`
- <a id="s-f03d7a5f7c"></a>Structured output: `none`
- <a id="s-6c04be0a0c"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-a9585a7694"></a>`stopped` | <a id="s-ee08eec785"></a>`{"kind":"service-runtime-returned"}` | <a id="s-56e36426bd"></a>`0` | <a id="s-ae3d913e3f"></a>all: `"no-command-result"` | <a id="s-153bd60e72"></a>all: `"noncontractual-runtime-log"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-ac0956a2b9"></a>`usage` | <a id="s-64db8e0ea2"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-c60bf907e8"></a>`2` | <a id="s-fb331ccc08"></a>all: `"empty"` | <a id="s-4414982e26"></a>all: `"noncontractual-usage-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --host](#s-61217926a3) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --port](#s-f51932dda3) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-a3aff67bd2"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-4420d619e1"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:riverhog-storage-adapter-aws](../../../evidence/sources.md#src-e3262944a6) — [reference/riverhog/storage/aws/src/riverhog\_storage\_adapter\_aws/app.py::&lt;module&gt;](../../../../../../reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py)
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/riverhog-storage-adapter-aws/allow_abbrev`
- `/external_contract/cli/riverhog-storage-adapter-aws/name`
- `/external_contract/cli/riverhog-storage-adapter-aws/parameters`
- `/external_contract/cli/riverhog-storage-adapter-aws/result_contract`
- `/external_contract/cli/riverhog-storage-adapter-aws/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/riverhog-storage-adapter-aws/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/riverhog-storage-adapter-aws/name`

<!-- exact-contract-value: 31481b10d105a4b563490b273664223f618de735f5677e3e4f114dd4a3b45b07 -->

```json
"riverhog-storage-adapter-aws"
```

### `/external_contract/cli/riverhog-storage-adapter-aws/parameters`

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

### `/external_contract/cli/riverhog-storage-adapter-aws/result_contract`

<!-- exact-contract-value: c714aab4e3866a620a04200bbad8bea93683d23ac7cc30527bdd21144f486d4b -->

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
  "identity": "riverhog-storage-adapter-aws-cli-result/root/v1",
  "profile_id": "riverhog-storage-adapter-aws-cli-runtime/v1",
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

### `/external_contract/cli/riverhog-storage-adapter-aws/terminating_controls`

<!-- exact-contract-value: edbb0ec869fec200540cba0e672b7d34dc6418956ea905136fcb0f23897c8b93 -->

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
      "distribution": "riverhog-storage-adapter-aws",
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
