# gogurt provider

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:gogurt:gogurt-provider:a4fbb2e670 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-09e2ec7ac3"></a>Parser name: `provider`
- <a id="s-899744cfca"></a>Subcommand selection: required.
- <a id="s-7c64d50d62"></a>Extra arguments at this parser: accepted. Subcommand selection and child parsing still apply.
- <a id="s-9f9b7daad0"></a>Options after positional arguments at this parser: left as arguments.
- <a id="s-c8c6caa459"></a>Unknown options at this parser: rejected.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-321a662cf0"></a>`help` | <a id="s-9fe22efe77"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-dacbc2247a"></a>`0` | <a id="s-88dbe5ee83"></a>`"noncontractual-framework-help"` | <a id="s-f957fb89b3"></a>`"empty"` |

## Governing policies

- <a id="pa-d90a2b2c5a"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:gogurt](../../../evidence/sources.md#src-3b2297c37d) — `reference/gogurt/application/src/gogurt/cli.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/gogurt/commands/provider/allow_extra_args`
- `/external_contract/cli/gogurt/commands/provider/allow_interspersed_args`
- `/external_contract/cli/gogurt/commands/provider/ignore_unknown_options`
- `/external_contract/cli/gogurt/commands/provider/name`
- `/external_contract/cli/gogurt/commands/provider/parameters`
- `/external_contract/cli/gogurt/commands/provider/subcommand_required`
- `/external_contract/cli/gogurt/commands/provider/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/gogurt/commands/provider/allow_extra_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/gogurt/commands/provider/allow_interspersed_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/gogurt/commands/provider/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/gogurt/commands/provider/name`

<!-- exact-contract-value: ae2250bb2563882a230d0bc3a65275643452aa6461380b2508ef449e9d752f91 -->

```json
"provider"
```

### `/external_contract/cli/gogurt/commands/provider/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/gogurt/commands/provider/subcommand_required`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/gogurt/commands/provider/terminating_controls`

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
