# stove0 scheduler

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-scheduler:51763ba6fc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-23be569f91"></a>Parser name: `scheduler`
- <a id="s-436b90fbc9"></a>Subcommand selection: required.
- <a id="s-d178135f6c"></a>Extra arguments at this parser: accepted. Subcommand selection and child parsing still apply.
- <a id="s-5cc591e133"></a>Options after positional arguments at this parser: left as arguments.
- <a id="s-25c06bda59"></a>Unknown options at this parser: rejected.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-fc56a2a295"></a>`help` | <a id="s-c29aac68cc"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-e507e6d97b"></a>`0` | <a id="s-bf5abd47f3"></a>`"noncontractual-framework-help"` | <a id="s-d100fb43ad"></a>`"empty"` |

## Governing policies

- <a id="pa-4fc3956827"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources.md#src-6203ae7d88) — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0/commands/scheduler/allow_extra_args`
- `/external_contract/cli/stove0/commands/scheduler/allow_interspersed_args`
- `/external_contract/cli/stove0/commands/scheduler/ignore_unknown_options`
- `/external_contract/cli/stove0/commands/scheduler/name`
- `/external_contract/cli/stove0/commands/scheduler/parameters`
- `/external_contract/cli/stove0/commands/scheduler/subcommand_required`
- `/external_contract/cli/stove0/commands/scheduler/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/scheduler/allow_extra_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0/commands/scheduler/allow_interspersed_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/scheduler/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/scheduler/name`

<!-- exact-contract-value: 877fc37669cb9960b62ee3b97e9e47bf8732dc50cc93fb34e447fa6318d23920 -->

```json
"scheduler"
```

### `/external_contract/cli/stove0/commands/scheduler/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/stove0/commands/scheduler/subcommand_required`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0/commands/scheduler/terminating_controls`

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
