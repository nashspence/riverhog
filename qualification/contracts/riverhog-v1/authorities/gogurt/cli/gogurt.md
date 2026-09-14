# gogurt

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:gogurt:gogurt:906ea30826 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-95ce7f0545"></a>Parser name: `gogurt`

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-c2278de9d5"></a>`help` | <a id="s-17d9584515"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-16ecc2b9b8"></a>`0` | <a id="s-4ac9696ef6"></a>`"noncontractual-framework-help"` | <a id="s-b411ea7995"></a>`"empty"` |
| <a id="s-f9d31e785c"></a>`version` | <a id="s-797224f5ce"></a>`{"kind":"option-present","options":["--version"]}` | <a id="s-9e2c17798a"></a>`0` | <a id="s-b360733a7b"></a>`{"distribution":"gogurt","kind":"installed-coordinated-release-version","serialization":"noncontractual"}` | <a id="s-526d0b0093"></a>`"empty"` |

## Governing policies

- <a id="pa-87debe85ec"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:gogurt](../../../evidence/sources.md#src-3b2297c37d) — `reference/gogurt/application/src/gogurt/cli.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/gogurt/name`
- `/external_contract/cli/gogurt/parameters`
- `/external_contract/cli/gogurt/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/gogurt/name`

<!-- exact-contract-value: d62ab33f625c3607530d1d3341d0733e45a093ae76a2fa164ab43c23774463bb -->

```json
"gogurt"
```

### `/external_contract/cli/gogurt/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/gogurt/terminating_controls`

<!-- exact-contract-value: 3f73674cb73ec4a87c5a15ffd64f2d2357e0c8804ad2cecf747339af135b6440 -->

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
  },
  {
    "exit_status": 0,
    "id": "version",
    "stderr": "empty",
    "stdout": {
      "distribution": "gogurt",
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
