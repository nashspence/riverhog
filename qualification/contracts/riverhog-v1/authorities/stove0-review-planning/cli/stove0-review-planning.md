# stove0-review-planning

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-review-planning:stove0-review-planning:a2e2686849 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-planning](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-dae9ec8755"></a>Parser name: `stove0-review-planning`

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-0d3817d373"></a>`help` | <a id="s-e4ec60d935"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-795490d3e3"></a>`0` | <a id="s-fd97008e96"></a>`"noncontractual-framework-help"` | <a id="s-1f058ba026"></a>`"empty"` |

### Result and failure contract

- <a id="s-b591048dbb"></a>Result identity: `stove0-review-planning-cli-result/root/v1`
- <a id="s-25b284cc0e"></a>Profile: `stove0-review-planning-cli/v1`
- <a id="s-c546bb1483"></a>Structured output: `always-json`
- <a id="s-b236ff411e"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-0e1a4ad46c"></a>`reported` | <a id="s-5464cc7104"></a>`{"kind":"contract-report-completed"}` | <a id="s-030349a6ea"></a>`0` | <a id="s-eeb2270368"></a>`json: stove0-review-contract-report/v1` | <a id="s-412d1eb2d2"></a>`all: empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-5fb3bb619b"></a>`usage` | <a id="s-8e15c1c4d7"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-6549be7b49"></a>`2` | <a id="s-2e941bb0af"></a>`all: empty` | <a id="s-69d9fcf861"></a>`all: noncontractual-usage-diagnostic` |

## Governing policies

- <a id="pa-0f04ec36cf"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0-review-planning](../../../evidence/sources.md#src-ae789ab860) — `reference/stove0/targets/review/planning/src/stove0_review_planning/conformance.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0-review-planning/name`
- `/external_contract/cli/stove0-review-planning/parameters`
- `/external_contract/cli/stove0-review-planning/result_contract`
- `/external_contract/cli/stove0-review-planning/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0-review-planning/name`

<!-- exact-contract-value: 5d8ffc9dd6189ed095a3b442507a319ce446bbd6db2c2de9e1dea956495a0614 -->

```json
"stove0-review-planning"
```

### `/external_contract/cli/stove0-review-planning/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/stove0-review-planning/result_contract`

<!-- exact-contract-value: b8492de7dc7e8409047017490e916e72e719b3102f7d37b2db362b2363ba11a4 -->

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
  "identity": "stove0-review-planning-cli-result/root/v1",
  "profile_id": "stove0-review-planning-cli/v1",
  "structured_output": "always-json",
  "success": [
    {
      "exit_status": 0,
      "id": "reported",
      "selected_by": {
        "kind": "contract-report-completed"
      },
      "stderr": {
        "all": "empty"
      },
      "stdout": {
        "json": {
          "identity": "stove0-review-contract-report/v1",
          "kind": "semantic-format"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/stove0-review-planning/terminating_controls`

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
