# review0-sampler-schemas

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:review0-sampler-lib:review0-sampler-schemas:339a653eee -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-lib](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-e71057f4bf"></a>Parser name: `review0-sampler-schemas`

| Field | Value |
|---|---|
| <a id="s-d1549765b4"></a>`parameters` | `[]` |
- <a id="s-53456583c6"></a>Unique long-option abbreviations: accepted.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-009835f766"></a>`help` | <a id="s-db40e30905"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-dfd445080e"></a>`0` | <a id="s-21cd0b8404"></a>`"noncontractual-framework-help"` | <a id="s-4592994412"></a>`"empty"` |
| <a id="s-f0e8277071"></a>`version` | <a id="s-b264ff1596"></a>`{"kind":"option-present","options":["--version"]}` | <a id="s-f068df54c0"></a>`0` | <a id="s-07de48e126"></a>`{"distribution":"review0-sampler-lib","kind":"installed-coordinated-release-version","serialization":"noncontractual"}` | <a id="s-5b17e73b19"></a>`"empty"` |

### Result and failure contract

- <a id="s-2ee04ddfc3"></a>Result identity: `review0-sampler-schemas-cli-result/root/v1`
- <a id="s-d6cb2643c3"></a>Profile: `review0-sampler-schemas-cli/v1`
- <a id="s-7478796f7d"></a>Structured output: `always-json`
- <a id="s-2e965e5d26"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-5e8b8a4ea0"></a>`emitted` | <a id="s-9bd393ac03"></a>`{"kind":"schema-bundle-emitted"}` | <a id="s-db34e08b24"></a>`0` | <a id="s-a8407b21d3"></a>json: [generated:review0-sampler protocol](../process-protocol/generated-review0-sampler-protocol.md) | <a id="s-ab97f2c0b8"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-0f239611ff"></a>`usage` | <a id="s-c44066cd0e"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-eaca158088"></a>`2` | <a id="s-09fcb243c1"></a>all: `"empty"` | <a id="s-a2964edb97"></a>all: `"noncontractual-usage-diagnostic"` |

## Governing policies

- <a id="pa-e225c4ef22"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:review0-sampler-schemas](../../../evidence/sources/authorities.md#src-439039a92a) — [some-implementations/stove0/review0/sampler/support/src/review0\_sampler\_lib/schemas.py::&lt;module&gt;](../../../../../../some-implementations/stove0/review0/sampler/support/src/review0_sampler_lib/schemas.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/review0-sampler-schemas/allow_abbrev`
- `/external_contract/cli/review0-sampler-schemas/name`
- `/external_contract/cli/review0-sampler-schemas/parameters`
- `/external_contract/cli/review0-sampler-schemas/result_contract`
- `/external_contract/cli/review0-sampler-schemas/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/review0-sampler-schemas/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/review0-sampler-schemas/name`

<!-- exact-contract-value: ddb7bb0f52e4012c88b22a240475a26d4c1bf3e4f117709e1a08efb009b5fcbe -->

```json
"review0-sampler-schemas"
```

### `/external_contract/cli/review0-sampler-schemas/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/review0-sampler-schemas/result_contract`

<!-- exact-contract-value: 4d9fcf23e4235af5b4fdd47f187c91519fd6522a83d3d667cbdcbc05ae3aaa1c -->

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
  "identity": "review0-sampler-schemas-cli-result/root/v1",
  "profile_id": "review0-sampler-schemas-cli/v1",
  "structured_output": "always-json",
  "success": [
    {
      "exit_status": 0,
      "id": "emitted",
      "selected_by": {
        "kind": "schema-bundle-emitted"
      },
      "stderr": {
        "all": "empty"
      },
      "stdout": {
        "json": {
          "authority": "generated:review0-sampler",
          "kind": "document-authority"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/review0-sampler-schemas/terminating_controls`

<!-- exact-contract-value: 335df2af25083f83c73a4da9864b883123434a963b040b0355ecd13eeb669e8c -->

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
  },
  {
    "exit_status": 0,
    "id": "version",
    "stderr": "empty",
    "stdout": {
      "distribution": "review0-sampler-lib",
      "kind": "installed-coordinated-release-version",
      "serialization": "noncontractual"
    },
    "trigger": {
      "kind": "option-present",
      "options": [
        "--version"
      ]
    }
  }
]
```

</details>
