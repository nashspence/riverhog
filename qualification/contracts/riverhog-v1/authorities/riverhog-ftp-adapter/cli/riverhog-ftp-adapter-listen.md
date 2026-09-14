# riverhog-ftp-adapter listen

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-ftp-adapter:riverhog-ftp-adapter-listen:bd530a9a23 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-74be9905cb"></a>Parser name: `listen`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-726cd2685b"></a>`source` | _StoreAction | yes |  | --source |
| <a id="s-1fa0ebe493"></a>`username` | _StoreAction | yes |  | --username |
| <a id="s-1d574ff9ed"></a>`password_file` | _StoreAction | yes | Path | --password-file |
| <a id="s-c920a0e180"></a>`host` | _StoreAction | no |  | --host |
| <a id="s-c5f9e26111"></a>`port` | _StoreAction | no | int | --port |
| <a id="s-5ab0d6d68c"></a>`passive_port_start` | _StoreAction | no | int | --passive-port-start |
| <a id="s-c3c71e786a"></a>`passive_port_end` | _StoreAction | no | int | --passive-port-end |
| <a id="s-afdf255e72"></a>`public_host` | _StoreAction | no |  | --public-host |
| <a id="s-ba15d7ef39"></a>`max_connections` | _StoreAction | no | int | --max-connections |
| <a id="s-9d17fa8370"></a>`max_connections_per_ip` | _StoreAction | no | int | --max-connections-per-ip |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-b390d20c71"></a>`help` | <a id="s-4b2f9abe0c"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-569142a293"></a>`0` | <a id="s-3afa2b8e14"></a>`"noncontractual-framework-help"` | <a id="s-0b40535171"></a>`"empty"` |

### Result and failure contract

- <a id="s-4370b398b3"></a>Result identity: `riverhog-ftp-adapter-cli-result/listen/v1`
- <a id="s-b502facb07"></a>Profile: `riverhog-ftp-adapter-cli-runtime/v1`
- <a id="s-731c5c7b95"></a>Structured output: `none`
- <a id="s-17342a62cf"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-6480809c91"></a>`stopped` | <a id="s-824feeff6b"></a>`{"kind":"service-runtime-returned"}` | <a id="s-6a005a8188"></a>`0` | <a id="s-8c583022a9"></a>all: `no-command-result` | <a id="s-c92cc1fc9d"></a>all: `noncontractual-runtime-log` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-6106114738"></a>`usage` | <a id="s-df76c739e5"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-feab2fe227"></a>`2` | <a id="s-a2da385836"></a>all: `empty` | <a id="s-0c2b6e6bd5"></a>all: `noncontractual-usage-diagnostic` |

## Governing policies

- <a id="pa-22462763aa"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:riverhog-ftp-adapter](../../../evidence/sources.md#src-303f765bca) — `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/app.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/riverhog-ftp-adapter/commands/listen/name`
- `/external_contract/cli/riverhog-ftp-adapter/commands/listen/parameters`
- `/external_contract/cli/riverhog-ftp-adapter/commands/listen/result_contract`
- `/external_contract/cli/riverhog-ftp-adapter/commands/listen/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/riverhog-ftp-adapter/commands/listen/name`

<!-- exact-contract-value: 3237ced043ee02810536760dbe5970a8ad7ba6591bc86fe3f7beb4530ad746c6 -->

```json
"listen"
```

### `/external_contract/cli/riverhog-ftp-adapter/commands/listen/parameters`

<!-- exact-contract-value: 16bec56e9da277c3cfdde2175af9cafb1d17d696bd2668c018e041094e381aac -->

```json
[
  {
    "dest": "source",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--source"
    ],
    "required": true
  },
  {
    "dest": "username",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--username"
    ],
    "required": true
  },
  {
    "dest": "password_file",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--password-file"
    ],
    "required": true,
    "type": "Path"
  },
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
    "default": 2121,
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
    "default": 30000,
    "dest": "passive_port_start",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--passive-port-start"
    ],
    "required": false,
    "type": "int"
  },
  {
    "default": 30039,
    "dest": "passive_port_end",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--passive-port-end"
    ],
    "required": false,
    "type": "int"
  },
  {
    "dest": "public_host",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--public-host"
    ],
    "required": false
  },
  {
    "default": 256,
    "dest": "max_connections",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--max-connections"
    ],
    "required": false,
    "type": "int"
  },
  {
    "default": 32,
    "dest": "max_connections_per_ip",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--max-connections-per-ip"
    ],
    "required": false,
    "type": "int"
  }
]
```

### `/external_contract/cli/riverhog-ftp-adapter/commands/listen/result_contract`

<!-- exact-contract-value: 44f4e21e2f98f8d51a9dfc943e67f161014bf8d9f7f15628b6b27363c3d971d9 -->

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
  "identity": "riverhog-ftp-adapter-cli-result/listen/v1",
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

### `/external_contract/cli/riverhog-ftp-adapter/commands/listen/terminating_controls`

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
