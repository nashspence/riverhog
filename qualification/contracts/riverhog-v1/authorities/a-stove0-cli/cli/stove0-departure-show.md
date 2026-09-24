# stove0 departure show

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-stove0-cli:stove0-departure-show:ea07dea3dc -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-109775e9cc"></a>Parser name: `show`
- <a id="s-7b5df2b738"></a>Extra arguments at this parser: rejected.
- <a id="s-839c8a67c3"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-e9ccd9e0a8"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-6442beec03"></a>`departure_id`<br>`departure_id` | required positional; 1 value | text | not recorded<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-95de2825ad"></a>`help` | <a id="s-6e239c0c50"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-395e69b78b"></a>`0` | <a id="s-336ef6d9c7"></a>`"noncontractual-framework-help"` | <a id="s-5aaa08f704"></a>`"empty"` |

### Result and failure contract

- <a id="s-b337ec981c"></a>Result identity: `stove0-cli-result/departure/show/v1`
- <a id="s-1ae3f81b7f"></a>Profile: `stove0-cli-human-json/v1`
- <a id="s-c8665233dc"></a>Structured output: `optional-json`
- <a id="s-1179fd0ef4"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-44369cc234"></a>`completed` | <a id="s-c32b1b2dd5"></a>`{"kind":"command-completed"}` | <a id="s-137d8a8fa8"></a>`0` | <a id="s-156ce837a8"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP get_departure_effect response 200](../../stove0/http-operations/get-v1-departure-effects-departure-id.md#s-d63131bb18) | <a id="s-b1bc04d158"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-9b198196b8"></a>`usage` | <a id="s-7b042f46a7"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-4a1550fcba"></a>`2` | <a id="s-975a78e462"></a>all: `"empty"` | <a id="s-07859df763"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-ee82e766fa"></a>`operational` | <a id="s-c919919379"></a>`{"kind":"application-error"}` | <a id="s-143989ec29"></a>`1` | <a id="s-e903d4a66b"></a>all: `"empty"` | <a id="s-0cd220c613"></a>all: `"noncontractual-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter departure_id](#s-6442beec03) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [GET /v1/departure-effects/{departure_id}](../../stove0/http-operations/get-v1-departure-effects-departure-id.md)
- [stove0_api_client.Stove0ApiClient.get_departure_effect](../../stove0-api-client/python/stove0-api-client-stove0apiclient-get-departure-effect.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-970eff4a76"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-9a4585d191"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources/authorities.md#src-6203ae7d88) — [some-implementations/stove0/application/client/src/a\_stove0\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/stove0/application/client/src/a_stove0_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/stove0/application/client/src/a\_stove0\_cli/main.py::show\_departure\_effect](../../../../../../some-implementations/stove0/application/client/src/a_stove0_cli/main.py#L286)

### Machine authority

- `/external_contract/cli/stove0/commands/departure/commands/show/allow_extra_args`
- `/external_contract/cli/stove0/commands/departure/commands/show/allow_interspersed_args`
- `/external_contract/cli/stove0/commands/departure/commands/show/ignore_unknown_options`
- `/external_contract/cli/stove0/commands/departure/commands/show/name`
- `/external_contract/cli/stove0/commands/departure/commands/show/parameters`
- `/external_contract/cli/stove0/commands/departure/commands/show/result_contract`
- `/external_contract/cli/stove0/commands/departure/commands/show/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/departure/commands/show/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/departure/commands/show/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0/commands/departure/commands/show/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/departure/commands/show/name`

<!-- exact-contract-value: 8f06acb02230bb5a194e0d7f4143d2ecaa508ef645f91340e0e7629981ca6044 -->

```json
"show"
```

### `/external_contract/cli/stove0/commands/departure/commands/show/parameters`

<!-- exact-contract-value: b5102ca84f7ef1595be1ad7d869387852dbb86077878f95c237bbbe9cef2f238 -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "departure_id",
    "nargs": 1,
    "options": [
      "departure_id"
    ],
    "required": true,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.StringParamType",
      "name": "text"
    }
  }
]
```

### `/external_contract/cli/stove0/commands/departure/commands/show/result_contract`

<!-- exact-contract-value: 790e933690f7c996dc4abce43d1fe9b85930f6608867fce4d3f5beefe2107f26 -->

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
    },
    {
      "exit_status": 1,
      "id": "operational",
      "selected_by": {
        "kind": "application-error"
      },
      "stderr": {
        "all": "noncontractual-diagnostic"
      },
      "stdout": {
        "all": "empty"
      }
    }
  ],
  "human_json_relationship": "same-semantic-result",
  "identity": "stove0-cli-result/departure/show/v1",
  "profile_id": "stove0-cli-human-json/v1",
  "structured_output": "optional-json",
  "success": [
    {
      "exit_status": 0,
      "id": "completed",
      "selected_by": {
        "kind": "command-completed"
      },
      "stderr": {
        "all": "empty"
      },
      "stdout": {
        "human": "noncontractual-presentation-of-command-result",
        "json": {
          "application": "stove0",
          "kind": "http-operation-response",
          "method": "GET",
          "operation_id": "get_departure_effect",
          "path": "/v1/departure-effects/{departure_id}",
          "schema": {
            "$ref": "#/components/schemas/DepartureEffectView"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/stove0/commands/departure/commands/show/terminating_controls`

<!-- exact-contract-value: 654ffd6937a42b17b4204e0750fd74bd42751d2241f4a4632015efb38811a79c -->

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
        "--help"
      ]
    }
  }
]
```

</details>
