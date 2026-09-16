# riverhog-ftp-adapter

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-ftp-adapter:riverhog-ftp-adapter:ff7f549ce2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-3b77079260"></a>Parser name: `riverhog-ftp-adapter`

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-90b07e9fd2"></a>`config`<br>`--config` | optional option; 1 value | Path | not recorded |
| <a id="s-f757f099cb"></a>`base_url`<br>`--base-url` | optional option; 1 value | not recorded | not recorded |
| <a id="s-4088337b61"></a>`token`<br>`--token` | optional option; 1 value | not recorded | not recorded |
| <a id="s-51acb7a74a"></a>`allow_insecure_http`<br>`--allow-insecure-http` | optional flag; 0 values | not recorded | not recorded |
| <a id="s-a4af6d5982"></a>`json`<br>`--json` | optional flag; 0 values | not recorded | `false` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-8356b5333e"></a>`help` | <a id="s-9934350cd8"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-23f9758a4d"></a>`0` | <a id="s-13c834dd4a"></a>`"noncontractual-framework-help"` | <a id="s-ef45850c29"></a>`"empty"` |
| <a id="s-8bf4e36baa"></a>`version` | <a id="s-2ffb30ee6d"></a>`{"kind":"option-present","options":["--version"]}` | <a id="s-0b5f6fe449"></a>`0` | <a id="s-8ba3f0256f"></a>`{"distribution":"riverhog-ftp-adapter","kind":"installed-coordinated-release-version","serialization":"noncontractual"}` | <a id="s-38425a7220"></a>`"empty"` |

### Result and failure contract

- <a id="s-0a777ed140"></a>Result identity: `riverhog-ftp-adapter-cli-result/root/v1`
- <a id="s-dcf5d24bcd"></a>Profile: `riverhog-ftp-adapter-cli-runtime/v1`
- <a id="s-905b260b99"></a>Structured output: `none`
- <a id="s-7e0c6bcb84"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-68b0044fae"></a>`stopped` | <a id="s-bf5a263ec7"></a>`{"kind":"service-runtime-returned"}` | <a id="s-ad2cb002db"></a>`0` | <a id="s-c629e04d01"></a>all: `no-command-result` | <a id="s-6b065c9d2c"></a>all: `noncontractual-runtime-log` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-00d5a3870f"></a>`usage` | <a id="s-e82b3868f9"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-5a3bbd4aa5"></a>`2` | <a id="s-1c0794da17"></a>all: `empty` | <a id="s-caa1bea57f"></a>all: `noncontractual-usage-diagnostic` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --config](#s-90b07e9fd2) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1 |
| [CLI parameter --base-url](#s-f757f099cb) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1 |
| [CLI parameter --token](#s-4088337b61) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1 |
| [CLI parameter --allow-insecure-http](#s-51acb7a74a) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0 |
| [CLI parameter --json](#s-a4af6d5982) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0 |

## Governing policies

- <a id="pa-b88e6bbdaa"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-4ab8861746"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:riverhog-ftp-adapter](../../../evidence/sources.md#src-303f765bca) — `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/app.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/riverhog-ftp-adapter/name`
- `/external_contract/cli/riverhog-ftp-adapter/parameters`
- `/external_contract/cli/riverhog-ftp-adapter/result_contract`
- `/external_contract/cli/riverhog-ftp-adapter/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/riverhog-ftp-adapter/name`

<!-- exact-contract-value: 7e14ae24d5b05761347a785bd381607ae0f3f00e29d17470741f0a64d8f9e3bc -->

```json
"riverhog-ftp-adapter"
```

### `/external_contract/cli/riverhog-ftp-adapter/parameters`

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

### `/external_contract/cli/riverhog-ftp-adapter/result_contract`

<!-- exact-contract-value: 4841cc8a8baf1c5c61c61dc23d21800923a8ed52d3f8a560013d4951f80f7581 -->

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
  "identity": "riverhog-ftp-adapter-cli-result/root/v1",
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

### `/external_contract/cli/riverhog-ftp-adapter/terminating_controls`

<!-- exact-contract-value: d5283c76b3ceb53528ac73d0b336ad6c7f248d07c7bab70b002bf456b31bbb08 -->

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
      "distribution": "riverhog-ftp-adapter",
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
