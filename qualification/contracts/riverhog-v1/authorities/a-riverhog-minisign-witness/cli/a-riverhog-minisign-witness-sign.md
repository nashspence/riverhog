# a-riverhog-minisign-witness sign

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-minisign-witness:a-riverhog-minisign-witness-sign:48b4b0f4e8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-minisign-witness](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-65702d347d"></a>Parser name: `sign`
- <a id="s-9d9ec72275"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-3adb6b25cf"></a>`secret_key`<br>`--secret-key` | required option; 1 value | Path | not recorded |
| <a id="s-7f46275496"></a>`public_key`<br>`--public-key` | required option; 1 value | Path | not recorded |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-ab280c237f"></a>`help` | <a id="s-27c48aa70d"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-09e346a41c"></a>`0` | <a id="s-159e5c9aa8"></a>`"noncontractual-framework-help"` | <a id="s-91f18098f1"></a>`"empty"` |

### Result and failure contract

- <a id="s-61ec12dc81"></a>Result identity: `a-riverhog-minisign-witness-cli-result/sign/v1`
- <a id="s-dd8c253791"></a>Profile: `a-riverhog-minisign-witness-cli-json/v1`
- <a id="s-73aacff5fc"></a>Structured output: `always-json`
- <a id="s-578c8289ae"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-866ef52d4f"></a>`completed` | <a id="s-f7bab570e5"></a>`{"kind":"command-completed"}` | <a id="s-421ae5645c"></a>`0` | <a id="s-ddd4f27a65"></a>json: [a-riverhog-minisign-witness-cli-sign/v1](#s-4f3c84627b) | <a id="s-c022cf697e"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-e1100df061"></a>`usage` | <a id="s-a10803a1bf"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-b06cb164f5"></a>`2` | <a id="s-0eb2467c69"></a>all: `"empty"` | <a id="s-85caf069fe"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-85e1e12a72"></a>`application-error` | <a id="s-a6894e9016"></a>`{"kind":"application-error"}` | <a id="s-65db833739"></a>`1` | <a id="s-ad8fc123f4"></a>all: `"empty"` | <a id="s-cf8ea793fe"></a>all: `"noncontractual-diagnostic"` |

### Local structured outputs


#### <a id="s-4f3c84627b"></a>`a-riverhog-minisign-witness-cli-sign/v1`

Applies to: completed · stdout (json).

<a id="s-abbaa21916"></a>

- <a id="s-349adc4704"></a>`type`: `"object"`
- <a id="s-dccb4ba161"></a>`additionalProperties`: `false`
- <a id="s-e96ad5126f"></a>`required`: `["signed_statement"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9bf6b5ff49"></a>`signed_statement` | yes | type=["string","null"] |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --secret-key](#s-3adb6b25cf) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --public-key](#s-7f46275496) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-3d203cdf3b"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-6bfde02a3a"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-minisign-witness](../../../evidence/sources/authorities.md#src-ab8103c6de) — [some-implementations/riverhog/applications/a-riverhog-minisign-witness/src/a\_riverhog\_minisign\_witness/cli.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-minisign-witness/src/a_riverhog_minisign_witness/cli.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-minisign-witness/commands/sign/allow_abbrev`
- `/external_contract/cli/a-riverhog-minisign-witness/commands/sign/name`
- `/external_contract/cli/a-riverhog-minisign-witness/commands/sign/parameters`
- `/external_contract/cli/a-riverhog-minisign-witness/commands/sign/result_contract`
- `/external_contract/cli/a-riverhog-minisign-witness/commands/sign/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-minisign-witness/commands/sign/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-minisign-witness/commands/sign/name`

<!-- exact-contract-value: a153f85505cf2bc69f05aed857711be35def4e520eadca3f7217c873644f5eb3 -->

```json
"sign"
```

### `/external_contract/cli/a-riverhog-minisign-witness/commands/sign/parameters`

<!-- exact-contract-value: 509c4f36b1051d128bb7eb34173e47c14cdb2fa96899c05ae715584da6009a8f -->

```json
[
  {
    "dest": "secret_key",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--secret-key"
    ],
    "required": true,
    "type": "Path"
  },
  {
    "dest": "public_key",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--public-key"
    ],
    "required": true,
    "type": "Path"
  }
]
```

### `/external_contract/cli/a-riverhog-minisign-witness/commands/sign/result_contract`

<!-- exact-contract-value: e5bc2187152aa1696ba289ed3077fa189e64f0dcdce8e84f82015556a346c5ed -->

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
  "identity": "a-riverhog-minisign-witness-cli-result/sign/v1",
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
          "identity": "a-riverhog-minisign-witness-cli-sign/v1",
          "kind": "cli-local-json-schema",
          "schema": {
            "additionalProperties": false,
            "properties": {
              "signed_statement": {
                "type": [
                  "string",
                  "null"
                ]
              }
            },
            "required": [
              "signed_statement"
            ],
            "type": "object"
          }
        }
      }
    }
  ]
}
```

### `/external_contract/cli/a-riverhog-minisign-witness/commands/sign/terminating_controls`

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
