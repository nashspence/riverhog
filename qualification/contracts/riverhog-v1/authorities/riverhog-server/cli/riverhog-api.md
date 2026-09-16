# riverhog-api

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-server:riverhog-api:9b5302929c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-395fe3aacf"></a>Parser name: `riverhog-api`
- <a id="s-bcf8f06216"></a>Subcommand selection: optional.
- <a id="s-410963d8dd"></a>Unique long-option abbreviations: accepted.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-c4b07f8425"></a>`help` | <a id="s-15093b1f45"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-c6bb1e86ea"></a>`0` | <a id="s-e992f6919c"></a>`"noncontractual-framework-help"` | <a id="s-48fa740b37"></a>`"empty"` |
| <a id="s-eee4a2022f"></a>`version` | <a id="s-3c3ce8749a"></a>`{"kind":"option-present","options":["--version"]}` | <a id="s-0f48a9ca23"></a>`0` | <a id="s-324ad62884"></a>`{"distribution":"riverhog-server","kind":"installed-coordinated-release-version","serialization":"noncontractual"}` | <a id="s-7ad462c70d"></a>`"empty"` |

### Result and failure contract

- <a id="s-375eddcf37"></a>Result identity: `riverhog-api-cli-result/root/v1`
- <a id="s-f8631770f5"></a>Profile: `riverhog-api-cli-runtime/v1`
- <a id="s-a388e9e06b"></a>Structured output: `none`
- <a id="s-f14e88dc7f"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-5a2b4ef233"></a>`stopped` | <a id="s-f2d5760826"></a>`{"kind":"service-runtime-returned"}` | <a id="s-8ec9193e1b"></a>`0` | <a id="s-e5c55a833f"></a>all: `no-command-result` | <a id="s-6cd3750fa6"></a>all: `noncontractual-runtime-log` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-342da844c2"></a>`usage` | <a id="s-6882d8f443"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-b6589d271d"></a>`2` | <a id="s-300cd69836"></a>all: `empty` | <a id="s-2cdf87c1e2"></a>all: `noncontractual-usage-diagnostic` |

## Governing policies

- <a id="pa-190c4aa3d6"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:riverhog-api](../../../evidence/sources.md#src-18139c42dd) — `riverhog/src/riverhog_api/app.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/riverhog-api/allow_abbrev`
- `/external_contract/cli/riverhog-api/name`
- `/external_contract/cli/riverhog-api/parameters`
- `/external_contract/cli/riverhog-api/result_contract`
- `/external_contract/cli/riverhog-api/subcommand_required`
- `/external_contract/cli/riverhog-api/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/riverhog-api/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/riverhog-api/name`

<!-- exact-contract-value: 8ba135ca7c417c43b4bd1033d1b770f0d7c20d6194a292f239fbfeb7b86fb360 -->

```json
"riverhog-api"
```

### `/external_contract/cli/riverhog-api/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/riverhog-api/result_contract`

<!-- exact-contract-value: e9f99c85245efbba36c69dc86a71e8e12309917fea93f7f774aeb8ef5766fa2e -->

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
  "identity": "riverhog-api-cli-result/root/v1",
  "profile_id": "riverhog-api-cli-runtime/v1",
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

### `/external_contract/cli/riverhog-api/subcommand_required`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/riverhog-api/terminating_controls`

<!-- exact-contract-value: 195793bdb467526cf7a0d5a252ee88d7b4d2ee00518b9c8a7f75bf542a6cd5db -->

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
      "distribution": "riverhog-server",
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
