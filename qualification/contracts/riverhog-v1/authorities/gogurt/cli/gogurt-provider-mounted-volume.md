# gogurt provider mounted-volume

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:gogurt:gogurt-provider-mounted-volume:b50912f90b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-d9ba8ed0af"></a>Parser name: `mounted-volume`

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-cd64177804"></a>`help` | <a id="s-ad6164b3ae"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-c92e64ebeb"></a>`0` | <a id="s-b7ca3a7931"></a>`"noncontractual-framework-help"` | <a id="s-34dc312412"></a>`"empty"` |

## Governing policies

- <a id="pa-905564dac2"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:gogurt](../../../evidence/sources.md#src-3b2297c37d) — `reference/gogurt/application/src/gogurt/cli.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/name`
- `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/parameters`
- `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/name`

<!-- exact-contract-value: e9c9ea664551c87759d7f3086386b5abd5faf3386e7b04e2118adc487be663c4 -->

```json
"mounted-volume"
```

### `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/terminating_controls`

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
