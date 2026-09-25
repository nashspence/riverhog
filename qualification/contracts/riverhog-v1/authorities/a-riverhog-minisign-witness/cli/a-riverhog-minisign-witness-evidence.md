# a-riverhog-minisign-witness evidence

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-minisign-witness:a-riverhog-minisign-witness-evidence:8ca86b1852 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-minisign-witness](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-5c6231dc69"></a>Parser name: `evidence`
- <a id="s-6a799826b1"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-74c5a5facc"></a>`digest` | required positional; 1 value | not recorded | not recorded |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-5e233bd27d"></a>`help` | <a id="s-7abb1b9bde"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-bceb1d1032"></a>`0` | <a id="s-6d0983ebe6"></a>`"noncontractual-framework-help"` | <a id="s-c08b95c872"></a>`"empty"` |

### Result and failure contract

- <a id="s-210a93d786"></a>Result identity: `a-riverhog-minisign-witness-cli-result/evidence/v1`
- <a id="s-920072a5ed"></a>Profile: `a-riverhog-minisign-witness-cli-json/v1`
- <a id="s-6bac035267"></a>Structured output: `always-json`
- <a id="s-d5b6acf077"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-db9e4701b4"></a>`completed` | <a id="s-c9d4500425"></a>`{"kind":"command-completed"}` | <a id="s-af2f3b6f2c"></a>`0` | <a id="s-35339ecf3c"></a>json: [a-riverhog-minisign-witness-cli-evidence/v1](#s-bec0b5c868) | <a id="s-fd18639145"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-e587e9b0b9"></a>`usage` | <a id="s-82201b772e"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-defeb1e5f9"></a>`2` | <a id="s-943898c7e1"></a>all: `"empty"` | <a id="s-6b9bf9ebf4"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-4fead78fc7"></a>`application-error` | <a id="s-83977a509e"></a>`{"kind":"application-error"}` | <a id="s-ce20227edd"></a>`1` | <a id="s-f3b0db17dc"></a>all: `"empty"` | <a id="s-932cd55fcb"></a>all: `"noncontractual-diagnostic"` |

### Local structured outputs


#### <a id="s-bec0b5c868"></a>`a-riverhog-minisign-witness-cli-evidence/v1`

Applies to: completed · stdout (json).

<a id="s-348302223c"></a>

- <a id="s-98b7cd5860"></a>`type`: `"object"`
- <a id="s-6a1fb5ad61"></a>`additionalProperties`: `false`
- <a id="s-5ed1fb9e8c"></a>`required`: `["digest","state","statement_base64","signature_base64","key_identity","error","attempts","due"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7108f36ba7"></a>`attempts` | yes | type="integer"; minimum=0 |  |
| <a id="s-b80baae62f"></a>`digest` | yes | type="string" |  |
| <a id="s-2b58ef37ea"></a>`due` | yes | type=["integer","null"]; minimum=0 |  |
| <a id="s-fb327330d3"></a>`error` | yes | type=["string","null"] |  |
| <a id="s-31586b07ca"></a>`key_identity` | yes | type=["string","null"] |  |
| <a id="s-1b9ac0d7e1"></a>`signature_base64` | yes | type=["string","null"] |  |
| <a id="s-139bb8bc9e"></a>`state` | yes | enum=["pending","signed","blocked"] |  |
| <a id="s-c39999e30d"></a>`statement_base64` | yes | type="string" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter 0](#s-74c5a5facc) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-0e29d0ec2c"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-f53ff10fec"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-minisign-witness](../../../evidence/sources/authorities.md#src-ab8103c6de) — [some-implementations/riverhog/applications/a-riverhog-minisign-witness/src/a\_riverhog\_minisign\_witness/cli.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-minisign-witness/src/a_riverhog_minisign_witness/cli.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-minisign-witness/commands/evidence/allow_abbrev`
- `/external_contract/cli/a-riverhog-minisign-witness/commands/evidence/name`
- `/external_contract/cli/a-riverhog-minisign-witness/commands/evidence/parameters`
- `/external_contract/cli/a-riverhog-minisign-witness/commands/evidence/result_contract`
- `/external_contract/cli/a-riverhog-minisign-witness/commands/evidence/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-minisign-witness/commands/evidence/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-minisign-witness/commands/evidence/name`

<!-- exact-contract-value: edecec53ab11f560816e75a17681eb83886beace01d7fb927d99707d3d031aac -->

```json
"evidence"
```

### `/external_contract/cli/a-riverhog-minisign-witness/commands/evidence/parameters`

<!-- exact-contract-value: 77121347de116990d37ff93ff27cd2f5f7aabac98f79343ea6a4022cfff061e2 -->

```json
[
  {
    "dest": "digest",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [],
    "required": true
  }
]
```

### `/external_contract/cli/a-riverhog-minisign-witness/commands/evidence/result_contract`

<!-- exact-contract-value: f247c4d8fdb782fee3bbf764a19b0df2129c8aaa1507acc1918f08ee6994c924 -->

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
      "id": "application-error",
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
  "human_json_relationship": "not-applicable",
  "identity": "a-riverhog-minisign-witness-cli-result/evidence/v1",
  "profile_id": "a-riverhog-minisign-witness-cli-json/v1",
  "structured_output": "always-json",
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
        "json": {
          "identity": "a-riverhog-minisign-witness-cli-evidence/v1",
          "kind": "cli-local-json-schema",
          "schema": {
            "additionalProperties": false,
            "properties": {
              "attempts": {
                "minimum": 0,
                "type": "integer"
              },
              "digest": {
                "type": "string"
              },
              "due": {
                "minimum": 0,
                "type": [
                  "integer",
                  "null"
                ]
              },
              "error": {
                "type": [
                  "string",
                  "null"
                ]
              },
              "key_identity": {
                "type": [
                  "string",
                  "null"
                ]
              },
              "signature_base64": {
                "type": [
                  "string",
                  "null"
                ]
              },
              "state": {
                "enum": [
                  "pending",
                  "signed",
                  "blocked"
                ]
              },
              "statement_base64": {
                "type": "string"
              }
            },
            "required": [
              "digest",
              "state",
              "statement_base64",
              "signature_base64",
              "key_identity",
              "error",
              "attempts",
              "due"
            ],
            "type": "object"
          }
        }
      }
    }
  ]
}
```

### `/external_contract/cli/a-riverhog-minisign-witness/commands/evidence/terminating_controls`

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

</details>
