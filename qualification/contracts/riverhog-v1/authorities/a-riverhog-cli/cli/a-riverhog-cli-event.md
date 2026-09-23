# a-riverhog-cli event

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-event:1b1a8226a0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-bbb242abe6"></a>Parser name: `event`

| Field | Value |
|---|---|
| <a id="s-656d8fe826"></a>`parameters` | `[]` |
- <a id="s-e735d8780a"></a>Subcommand selection: required.
- <a id="s-dad25f1268"></a>Extra arguments at this parser: accepted. Subcommand selection and child parsing still apply.
- <a id="s-ce46865078"></a>Options after positional arguments at this parser: left as arguments.
- <a id="s-d61b8b810a"></a>Unknown options at this parser: rejected.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-434e497065"></a>`help` | <a id="s-53f79f4ddf"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-745b0554e5"></a>`0` | <a id="s-892cf025f9"></a>`"noncontractual-framework-help"` | <a id="s-ac1e7e56ad"></a>`"empty"` |

## Governing policies

- <a id="pa-2d94c909f1"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/event/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/event/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/event/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/event/name`
- `/external_contract/cli/a-riverhog-cli/commands/event/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/event/subcommand_required`
- `/external_contract/cli/a-riverhog-cli/commands/event/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/event/allow_extra_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/event/allow_interspersed_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/event/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/event/name`

<!-- exact-contract-value: 8f6e29cfb2f0df76f9f90f221edf3a7b4cfc6ba2e409bb0a78a2c031afd78580 -->

```json
"event"
```

### `/external_contract/cli/a-riverhog-cli/commands/event/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/a-riverhog-cli/commands/event/subcommand_required`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/event/terminating_controls`

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
