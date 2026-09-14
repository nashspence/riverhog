# riverhog-ftp-adapter

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-ftp-adapter:riverhog-ftp-adapter:f74a46f926 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-3b77079260"></a>Parser name: `riverhog-ftp-adapter`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-90b07e9fd2"></a>`version` | _VersionAction | no |  | --version |
| <a id="s-f757f099cb"></a>`config` | _StoreAction | no | Path | --config |
| <a id="s-4088337b61"></a>`base_url` | _StoreAction | no |  | --base-url |
| <a id="s-51acb7a74a"></a>`token` | _StoreAction | no |  | --token |
| <a id="s-a4af6d5982"></a>`allow_insecure_http` | _StoreTrueAction | no |  | --allow-insecure-http |
| <a id="s-7d46672345"></a>`json` | _StoreTrueAction | no |  | --json |

### Result and failure contract

- <a id="s-0a777ed140"></a>Result identity: `riverhog-ftp-adapter-cli-result/root/v1`
- <a id="s-dcf5d24bcd"></a>Profile: `riverhog-ftp-adapter-cli-runtime/v1`
- <a id="s-905b260b99"></a>Structured output: `none`
- <a id="s-7e0c6bcb84"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-68b0044fae"></a>`stopped` | <a id="s-ad2cb002db"></a>`0` | <a id="s-c629e04d01"></a>`{"all":"no-command-result"}` | <a id="s-6b065c9d2c"></a>`{"all":"noncontractual-runtime-log"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-00d5a3870f"></a>`usage` | <a id="s-5a3bbd4aa5"></a>`2` | <a id="s-1c0794da17"></a>`{"all":"empty"}` | <a id="s-caa1bea57f"></a>`{"all":"noncontractual-usage-diagnostic"}` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --version](#s-90b07e9fd2) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --allow-insecure-http](#s-a4af6d5982) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --json](#s-7d46672345) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-d332ebdc7d"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-c7830a8157"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

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

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/riverhog-ftp-adapter/name`

<!-- exact-contract-value: 7e14ae24d5b05761347a785bd381607ae0f3f00e29d17470741f0a64d8f9e3bc -->

```json
"riverhog-ftp-adapter"
```

### `/external_contract/cli/riverhog-ftp-adapter/parameters`

<!-- exact-contract-value: 5b0240d1d5a69359f4143950fdd586f7ee977d825a9f22f78b4493462265d79a -->

```json
[
  {
    "dest": "version",
    "kind": "_VersionAction",
    "nargs": 0,
    "options": [
      "--version"
    ],
    "required": false
  },
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

<!-- exact-contract-value: 11a6b39479a4b2fdbae54362cd97329c3d5390f7a16879dce4891998b42e6178 -->

```json
{
  "failures": [
    {
      "exit_status": 2,
      "id": "usage",
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
