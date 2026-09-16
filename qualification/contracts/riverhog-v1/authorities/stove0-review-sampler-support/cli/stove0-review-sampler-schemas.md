# stove0-review-sampler-schemas

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-review-sampler-support:stove0-review-sampler-schemas:5295047520 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-support](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-faa7bf9354"></a>Parser name: `stove0-review-sampler-schemas`
- <a id="s-90b0ada507"></a>Unique long-option abbreviations: accepted.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-727e79b4a4"></a>`help` | <a id="s-a152d931fa"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-1143d733ad"></a>`0` | <a id="s-c31755c46c"></a>`"noncontractual-framework-help"` | <a id="s-2210ab0ae9"></a>`"empty"` |
| <a id="s-c479b6ab0c"></a>`version` | <a id="s-9f416db306"></a>`{"kind":"option-present","options":["--version"]}` | <a id="s-3da2dfb528"></a>`0` | <a id="s-1297cc6642"></a>`{"distribution":"stove0-review-sampler-support","kind":"installed-coordinated-release-version","serialization":"noncontractual"}` | <a id="s-916ad14b65"></a>`"empty"` |

### Result and failure contract

- <a id="s-4b5a5f62bd"></a>Result identity: `stove0-review-sampler-schemas-cli-result/root/v1`
- <a id="s-3201b6e3d1"></a>Profile: `stove0-review-sampler-schemas-cli/v1`
- <a id="s-1a4eacf428"></a>Structured output: `always-json`
- <a id="s-f3ff1fa2df"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-a6da464362"></a>`emitted` | <a id="s-37a58d85d0"></a>`{"kind":"schema-bundle-emitted"}` | <a id="s-9c0954e1dc"></a>`0` | <a id="s-fc7431ee35"></a>json: [generated:stove0-review-sampler protocol](../process-protocol/generated-stove0-review-sampler-protocol.md) | <a id="s-089e853279"></a>all: `empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-f8dd902aa6"></a>`usage` | <a id="s-a67815c66f"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-14846d56db"></a>`2` | <a id="s-682c9abb41"></a>all: `empty` | <a id="s-5f0da7803b"></a>all: `noncontractual-usage-diagnostic` |

## Governing policies

- <a id="pa-7d5ccbfbd2"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0-review-sampler-schemas](../../../evidence/sources.md#src-a75c35f0c8) — `reference/stove0/targets/review/sampler/support/src/stove0_review_sampler_support/schemas.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0-review-sampler-schemas/allow_abbrev`
- `/external_contract/cli/stove0-review-sampler-schemas/name`
- `/external_contract/cli/stove0-review-sampler-schemas/parameters`
- `/external_contract/cli/stove0-review-sampler-schemas/result_contract`
- `/external_contract/cli/stove0-review-sampler-schemas/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0-review-sampler-schemas/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0-review-sampler-schemas/name`

<!-- exact-contract-value: 162a3ef3dc87aa1b3b1b2b34823d4702440ce3754ea9e31d325df3f59ec24a0c -->

```json
"stove0-review-sampler-schemas"
```

### `/external_contract/cli/stove0-review-sampler-schemas/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/stove0-review-sampler-schemas/result_contract`

<!-- exact-contract-value: 8973f6b156a39ab2bfb781f886f0f861bf48f9c82f7d616387b74ee7f56e89f9 -->

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
  "identity": "stove0-review-sampler-schemas-cli-result/root/v1",
  "profile_id": "stove0-review-sampler-schemas-cli/v1",
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
          "authority": "generated:stove0-review-sampler",
          "kind": "document-authority"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/stove0-review-sampler-schemas/terminating_controls`

<!-- exact-contract-value: fcddd733b548f7fdee3021b986cc2c4b7650c7a1a689657a12af45cc7a2137de -->

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
      "distribution": "stove0-review-sampler-support",
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
