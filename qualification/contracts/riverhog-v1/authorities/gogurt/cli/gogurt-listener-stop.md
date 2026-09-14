# gogurt listener stop

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:gogurt:gogurt-listener-stop:588a892910 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-b8a05038de"></a>Parser name: `stop`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-8a6e9e8180"></a>`listener_host_provider` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --listener-host-provider |
| <a id="s-9964b5c031"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Result and failure contract

- <a id="s-e75c2fc877"></a>Result identity: `gogurt-cli-result/listener/stop/v1`
- <a id="s-b0f82d156c"></a>Profile: `gogurt-cli-human-json/v1`
- <a id="s-a21f964eb3"></a>Structured output: `optional-json`
- <a id="s-1329e8ef07"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-413bf5ade1"></a>`completed` | <a id="s-d7087fdf5d"></a>`0` | <a id="s-d10a50bdd8"></a>`{"human":"noncontractual-presentation-of-command-result","json":"named-command-result"}` | <a id="s-ed27c8de11"></a>`{"all":"empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-97035ade3f"></a>`usage` | <a id="s-b719577371"></a>`2` | <a id="s-3e2f239655"></a>`{"all":"empty"}` | <a id="s-0aeb4e772d"></a>`{"all":"noncontractual-usage-diagnostic"}` |
| <a id="s-34ad9f46fd"></a>`operational` | <a id="s-6074c71542"></a>`1` | <a id="s-c5185a10e4"></a>`{"human":"empty","json":"gogurt-cli-error/v1"}` | <a id="s-8cdb0c18a5"></a>`{"human":"noncontractual-diagnostic","json":"empty"}` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-9964b5c031) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --listener-host-provider](#s-8a6e9e8180) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-3239e7f17d"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-e9529e12ed"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:gogurt](../../../evidence/sources.md#src-3b2297c37d) — `reference/gogurt/application/src/gogurt/cli.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/gogurt/commands/listener/commands/stop/name`
- `/external_contract/cli/gogurt/commands/listener/commands/stop/parameters`
- `/external_contract/cli/gogurt/commands/listener/commands/stop/result_contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/gogurt/commands/listener/commands/stop/name`

<!-- exact-contract-value: 6f2be2fe58ca2ba48e809c9588269fb710ce2535ba22acafb1123cdbb7421a02 -->

```json
"stop"
```

### `/external_contract/cli/gogurt/commands/listener/commands/stop/parameters`

<!-- exact-contract-value: fe7b56d2906ac79e76d31abc11e3bbc7c7ed0b66ee89c57133a60c8c1242d2fb -->

```json
[
  {
    "count": false,
    "envvar": "GOGURT_LISTENER_HOST_PROVIDER",
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "listener_host_provider",
    "nargs": 1,
    "options": [
      "--listener-host-provider"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.StringParamType",
      "name": "text"
    }
  },
  {
    "count": false,
    "default": false,
    "envvar": null,
    "is_flag": true,
    "kind": "TyperOption",
    "multiple": false,
    "name": "json_mode",
    "nargs": 1,
    "options": [
      "--json"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.BoolParamType",
      "name": "boolean"
    }
  }
]
```

### `/external_contract/cli/gogurt/commands/listener/commands/stop/result_contract`

<!-- exact-contract-value: 67d4114fb36e2e32803614f69be07768df37273ee70d785d09b599363bbc5d45 -->

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
    },
    {
      "exit_status": 1,
      "id": "operational",
      "stderr": {
        "human": "noncontractual-diagnostic",
        "json": "empty"
      },
      "stdout": {
        "human": "empty",
        "json": "gogurt-cli-error/v1"
      }
    }
  ],
  "human_json_relationship": "same-semantic-result",
  "identity": "gogurt-cli-result/listener/stop/v1",
  "profile_id": "gogurt-cli-human-json/v1",
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
