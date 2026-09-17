# stove0 work

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-work:9bbecab3cb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-535fc93ba7"></a>Parser name: `work`

| Field | Value |
|---|---|
| <a id="s-7a1a9210f0"></a>`parameters` | `[]` |
- <a id="s-1527a85edd"></a>Subcommand selection: required.
- <a id="s-c0ec6efa39"></a>Extra arguments at this parser: accepted. Subcommand selection and child parsing still apply.
- <a id="s-fa3192d00d"></a>Options after positional arguments at this parser: left as arguments.
- <a id="s-96b6401c63"></a>Unknown options at this parser: rejected.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-fbee11705c"></a>`help` | <a id="s-9247b327a2"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-4358845ad6"></a>`0` | <a id="s-d387685f44"></a>`"noncontractual-framework-help"` | <a id="s-8afd65e0ef"></a>`"empty"` |

## Governing policies

- <a id="pa-42bcfbfddc"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources/authorities.md#src-6203ae7d88) — [reference/stove0/application/client/src/stove0\_cli/main.py::&lt;module&gt;](../../../../../../reference/stove0/application/client/src/stove0_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/stove0/commands/work/allow_extra_args`
- `/external_contract/cli/stove0/commands/work/allow_interspersed_args`
- `/external_contract/cli/stove0/commands/work/ignore_unknown_options`
- `/external_contract/cli/stove0/commands/work/name`
- `/external_contract/cli/stove0/commands/work/parameters`
- `/external_contract/cli/stove0/commands/work/subcommand_required`
- `/external_contract/cli/stove0/commands/work/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/work/allow_extra_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0/commands/work/allow_interspersed_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/work/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/work/name`

<!-- exact-contract-value: dd3e8955d3a5ffeb7a7d3db22f06e294f71f57f1b717e9ae0ac76566e1a317da -->

```json
"work"
```

### `/external_contract/cli/stove0/commands/work/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/stove0/commands/work/subcommand_required`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0/commands/work/terminating_controls`

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
