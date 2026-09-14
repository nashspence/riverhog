# riverhog-storage-adapter-conformance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-storage-adapter-support:riverhog-storage-adapter-conformance:0a062baac0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-0d03ef1777"></a>Parser name: `riverhog-storage-adapter-conformance`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-da9cd0d5da"></a>`base_url` | _StoreAction | yes |  | --base-url |
| <a id="s-4db4323e5a"></a>`token_file` | _StoreAction | yes | Path | --token-file |
| <a id="s-76ce9fec00"></a>`object_prefix` | _StoreAction | yes |  | --object-prefix |
| <a id="s-785a5f2564"></a>`allow_insecure_http` | _StoreTrueAction | no |  | --allow-insecure-http |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-6869fb1242"></a>`help` | <a id="s-94bcbeb729"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-fa392b722c"></a>`0` | <a id="s-06d31a9dcb"></a>`"noncontractual-framework-help"` | <a id="s-fa39933834"></a>`"empty"` |

### Result and failure contract

- <a id="s-f9615a8394"></a>Result identity: `riverhog-storage-adapter-conformance-cli-result/root/v1`
- <a id="s-9b278bfaf6"></a>Profile: `riverhog-storage-adapter-conformance-cli/v1`
- <a id="s-96e3ff49ca"></a>Structured output: `always-json`
- <a id="s-f6d1e89bdf"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-0779825aae"></a>`conformant` | <a id="s-3356217d42"></a>`{"kind":"conformance-completed"}` | <a id="s-a511e84d49"></a>`0` | <a id="s-545267f90c"></a>json: [generated:riverhog-storage-adapter: StorageAdapterConformanceResult](../process-protocol-schemas/generated-riverhog-storage-adapter-storageadapterconformanceresult.md) | <a id="s-d8d26cc42e"></a>all: `empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-7d57e24d70"></a>`usage` | <a id="s-6b135baa08"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-4a1fdc4dd8"></a>`2` | <a id="s-a81996dea2"></a>all: `empty` | <a id="s-c3045d7639"></a>all: `noncontractual-usage-diagnostic` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --allow-insecure-http](#s-785a5f2564) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-8c3ae129c3"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-746de0798f"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:riverhog-storage-adapter-conformance](../../../evidence/sources.md#src-7ab923569b) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/conformance.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/riverhog-storage-adapter-conformance/name`
- `/external_contract/cli/riverhog-storage-adapter-conformance/parameters`
- `/external_contract/cli/riverhog-storage-adapter-conformance/result_contract`
- `/external_contract/cli/riverhog-storage-adapter-conformance/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/riverhog-storage-adapter-conformance/name`

<!-- exact-contract-value: 98d8fde2010458c2409a2fc87e33c689b1aca3836ae3f679276be44504d4ca90 -->

```json
"riverhog-storage-adapter-conformance"
```

### `/external_contract/cli/riverhog-storage-adapter-conformance/parameters`

<!-- exact-contract-value: 372bdc5b4e66958de053a3c608e89c26fd45b77348f19a9cb0ecdf6c8ac8f31f -->

```json
[
  {
    "dest": "base_url",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--base-url"
    ],
    "required": true
  },
  {
    "dest": "token_file",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--token-file"
    ],
    "required": true,
    "type": "Path"
  },
  {
    "dest": "object_prefix",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--object-prefix"
    ],
    "required": true
  },
  {
    "default": false,
    "dest": "allow_insecure_http",
    "kind": "_StoreTrueAction",
    "nargs": 0,
    "options": [
      "--allow-insecure-http"
    ],
    "required": false
  }
]
```

### `/external_contract/cli/riverhog-storage-adapter-conformance/result_contract`

<!-- exact-contract-value: 8f9369a0b3894d42ed58104a115a9acb5381def19e71b617277f68d35b04a77e -->

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
  "identity": "riverhog-storage-adapter-conformance-cli-result/root/v1",
  "profile_id": "riverhog-storage-adapter-conformance-cli/v1",
  "structured_output": "always-json",
  "success": [
    {
      "exit_status": 0,
      "id": "conformant",
      "selected_by": {
        "kind": "conformance-completed"
      },
      "stderr": {
        "all": "empty"
      },
      "stdout": {
        "json": {
          "authority": "generated:riverhog-storage-adapter",
          "definition": "StorageAdapterConformanceResult",
          "kind": "schema-authority"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/riverhog-storage-adapter-conformance/terminating_controls`

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
