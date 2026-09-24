# stove0 departure policy rebaseline

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-stove0-cli:stove0-departure-policy-rebaseline:91855d399b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-1d9dd3cc1d"></a>Parser name: `rebaseline`
- <a id="s-9f5f538c8c"></a>Extra arguments at this parser: rejected.
- <a id="s-627b897782"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-ee95c48775"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-e6fa0ef5ee"></a>`policy_id`<br>`policy_id` | required positional; 1 value | text | not recorded<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-21615bacac"></a>`help` | <a id="s-f1102d82cb"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-c4d9b53585"></a>`0` | <a id="s-8480a35114"></a>`"noncontractual-framework-help"` | <a id="s-d94465be2b"></a>`"empty"` |

### Result and failure contract

- <a id="s-915636af30"></a>Result identity: `stove0-cli-result/departure/policy/rebaseline/v1`
- <a id="s-37ffef4af3"></a>Profile: `stove0-cli-human-json/v1`
- <a id="s-8317918dbd"></a>Structured output: `optional-json`
- <a id="s-524994f389"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-c86e16a89f"></a>`completed` | <a id="s-fe83dccfd9"></a>`{"kind":"command-completed"}` | <a id="s-dbf4c44100"></a>`0` | <a id="s-34bdc333b5"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP rebaseline_departure_policy response 200](../../stove0/http-operations/post-v1-departure-policies-policy-id-rebaseline.md#s-6600a6f1e7) | <a id="s-9efb07ec0a"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-353eace873"></a>`usage` | <a id="s-0a3285173a"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-c46368e30c"></a>`2` | <a id="s-48145f8bb4"></a>all: `"empty"` | <a id="s-4eb1e679d6"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-47b1a93c26"></a>`operational` | <a id="s-c90ac2c4ff"></a>`{"kind":"application-error"}` | <a id="s-67d766db60"></a>`1` | <a id="s-d21fb8b700"></a>all: `"empty"` | <a id="s-b83402aae8"></a>all: `"noncontractual-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter policy_id](#s-e6fa0ef5ee) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [POST /v1/departure-policies/{policy_id}:rebaseline](../../stove0/http-operations/post-v1-departure-policies-policy-id-rebaseline.md)
- [stove0_api_client.Stove0ApiClient.rebaseline_departure_policy](../../stove0-api-client/python/stove0-api-client-stove0apiclient-rebaseline-departure-policy.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-7f8d90f521"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-ee5e47e558"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources/authorities.md#src-6203ae7d88) — [some-implementations/stove0/application/client/src/a\_stove0\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/stove0/application/client/src/a_stove0_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/stove0/application/client/src/a\_stove0\_cli/main.py::rebaseline\_departure\_policy](../../../../../../some-implementations/stove0/application/client/src/a_stove0_cli/main.py#L266)

### Machine authority

- `/external_contract/cli/stove0/commands/departure/commands/policy/commands/rebaseline/allow_extra_args`
- `/external_contract/cli/stove0/commands/departure/commands/policy/commands/rebaseline/allow_interspersed_args`
- `/external_contract/cli/stove0/commands/departure/commands/policy/commands/rebaseline/ignore_unknown_options`
- `/external_contract/cli/stove0/commands/departure/commands/policy/commands/rebaseline/name`
- `/external_contract/cli/stove0/commands/departure/commands/policy/commands/rebaseline/parameters`
- `/external_contract/cli/stove0/commands/departure/commands/policy/commands/rebaseline/result_contract`
- `/external_contract/cli/stove0/commands/departure/commands/policy/commands/rebaseline/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/departure/commands/policy/commands/rebaseline/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/departure/commands/policy/commands/rebaseline/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0/commands/departure/commands/policy/commands/rebaseline/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/departure/commands/policy/commands/rebaseline/name`

<!-- exact-contract-value: 6d6afe02995e3b8523f798f248dce3c58f9efae832d68eb5f8626c5ebdbf8423 -->

```json
"rebaseline"
```

### `/external_contract/cli/stove0/commands/departure/commands/policy/commands/rebaseline/parameters`

<!-- exact-contract-value: c2ba15d022e17d87c4fef6b4fc54fd47ee4a58f77e072e2fddfb95ae64e3222a -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "policy_id",
    "nargs": 1,
    "options": [
      "policy_id"
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

### `/external_contract/cli/stove0/commands/departure/commands/policy/commands/rebaseline/result_contract`

<!-- exact-contract-value: f53fbec1fc38343ad3f327398e3b25e4c97e1aa525ba98b6b4854deb9100f9a8 -->

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
  "identity": "stove0-cli-result/departure/policy/rebaseline/v1",
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
          "method": "POST",
          "operation_id": "rebaseline_departure_policy",
          "path": "/v1/departure-policies/{policy_id}:rebaseline",
          "schema": {
            "$ref": "#/components/schemas/DeparturePolicyStatus"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/stove0/commands/departure/commands/policy/commands/rebaseline/terminating_controls`

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
