# riverhog-ftp-adapter serve

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-ftp-adapter:riverhog-ftp-adapter-serve:14c1611198 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-cf31bb6074"></a>Parser name: `serve`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-8ae0b92557"></a>`host` | _StoreAction | no |  | --host |
| <a id="s-cb5e793bc3"></a>`port` | _StoreAction | no | int | --port |

### Result and failure contract

- <a id="s-71daf1cf85"></a>Result identity: `riverhog-ftp-adapter-cli-result/serve/v1`
- <a id="s-79d5ed72fc"></a>Profile: `riverhog-ftp-adapter-cli-runtime/v1`
- <a id="s-79dd6727dd"></a>Structured output: `none`
- <a id="s-29180db335"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-c1f2b3d923"></a>`stopped` | <a id="s-cbdc2fa32a"></a>`0` | <a id="s-32e5231259"></a>`{"all":"no-command-result"}` | <a id="s-9fa98217a0"></a>`{"all":"noncontractual-runtime-log"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-2d98dbad82"></a>`usage` | <a id="s-0ec318e545"></a>`2` | <a id="s-8a0b4eec4b"></a>`{"all":"empty"}` | <a id="s-95095cb125"></a>`{"all":"noncontractual-usage-diagnostic"}` |

## Governing policies

- <a id="pa-1aed0d05b5"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:riverhog-ftp-adapter](../../../evidence/sources.md#src-303f765bca) — `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/app.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/riverhog-ftp-adapter/commands/serve/name`
- `/external_contract/cli/riverhog-ftp-adapter/commands/serve/parameters`
- `/external_contract/cli/riverhog-ftp-adapter/commands/serve/result_contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

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

<!-- exact-contract-value: 86474a4f6fadf1c3640481f94763f9e8ee9a82fd261069373f024f22627e15b4 -->

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
  "identity": "riverhog-ftp-adapter-cli-result/serve/v1",
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
