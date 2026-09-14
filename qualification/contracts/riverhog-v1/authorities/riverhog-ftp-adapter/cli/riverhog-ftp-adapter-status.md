# riverhog-ftp-adapter status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-ftp-adapter:riverhog-ftp-adapter-status:e3396367b3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-f81d8fd65a"></a>Parser name: `status`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-f54fd55b02"></a>`page_size` | _StoreAction | no | int | --page-size |
| <a id="s-2a51aae1b2"></a>`page_token` | _StoreAction | no |  | --page-token |

### Result and failure contract

- <a id="s-e4422efdd8"></a>Result identity: `riverhog-ftp-adapter-cli-result/status/v1`
- <a id="s-1bb35bd7c8"></a>Profile: `riverhog-ftp-adapter-cli-human-json/v1`
- <a id="s-0b068eb1e0"></a>Structured output: `optional-json`
- <a id="s-3222b25064"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-c0c37f0681"></a>`completed` | <a id="s-8c1fb79da4"></a>`0` | <a id="s-054c956010"></a>`{"human":"noncontractual-presentation-of-command-result","json":"named-command-result"}` | <a id="s-791b0f86eb"></a>`{"all":"empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-04d532acf3"></a>`usage` | <a id="s-71989a06f8"></a>`2` | <a id="s-53ee405f4e"></a>`{"all":"empty"}` | <a id="s-fccc131e6b"></a>`{"all":"noncontractual-usage-diagnostic"}` |

## Maintained corroboration

### Related interface records

- [GET /v1/status](../http-operations/get-v1-status.md)

## Governing policies

- <a id="pa-8726bc62d2"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:riverhog-ftp-adapter](../../../evidence/sources.md#src-303f765bca) — `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/app.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/riverhog-ftp-adapter/commands/status/name`
- `/external_contract/cli/riverhog-ftp-adapter/commands/status/parameters`
- `/external_contract/cli/riverhog-ftp-adapter/commands/status/result_contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/riverhog-ftp-adapter/commands/status/name`

<!-- exact-contract-value: cfc31bcc34ed7f4cc7895026ae8a54f0494f73757e9f914d0f6ed90f9bc34f51 -->

```json
"status"
```

### `/external_contract/cli/riverhog-ftp-adapter/commands/status/parameters`

<!-- exact-contract-value: 9c78a4249746cc77ff1ad2537e381ac766e9b1e5866cda7e926f55bfb3973828 -->

```json
[
  {
    "default": 25,
    "dest": "page_size",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--page-size"
    ],
    "required": false,
    "type": "int"
  },
  {
    "dest": "page_token",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--page-token"
    ],
    "required": false
  }
]
```

### `/external_contract/cli/riverhog-ftp-adapter/commands/status/result_contract`

<!-- exact-contract-value: dc39e1dd06fd366917ab3dd2b4f0fd98257cdfa38506ac8cc1e44897d53cefe1 -->

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
  "human_json_relationship": "same-semantic-result",
  "identity": "riverhog-ftp-adapter-cli-result/status/v1",
  "profile_id": "riverhog-ftp-adapter-cli-human-json/v1",
  "structured_output": "optional-json",
  "success": [
    {
      "exit_status": 0,
      "id": "completed",
      "stderr": {
        "all": "empty"
      },
      "stdout": {
        "human": "noncontractual-presentation-of-command-result",
        "json": "named-command-result"
      }
    }
  ]
}
```
