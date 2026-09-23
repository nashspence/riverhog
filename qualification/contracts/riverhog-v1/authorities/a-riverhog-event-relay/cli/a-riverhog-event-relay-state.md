# a-riverhog-event-relay state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-event-relay:a-riverhog-event-relay-state:8f7bf66cf1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-event-relay](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-4f30794438"></a>Parser name: `state`

| Field | Value |
|---|---|
| <a id="s-f7100bf579"></a>`parameters` | `[]` |
- <a id="s-91b905689a"></a>Subcommand selection: required.
- <a id="s-6f8c51ac56"></a>Unique long-option abbreviations: accepted.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-1a39cf7f0b"></a>`help` | <a id="s-2ccb6ebe1c"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-ce78f2e46a"></a>`0` | <a id="s-769fa8f478"></a>`"noncontractual-framework-help"` | <a id="s-a2f7f3feca"></a>`"empty"` |

## Governing policies

- <a id="pa-3b4f00f542"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-event-relay](../../../evidence/sources/authorities.md#src-79ff0dba62) — [some-implementations/riverhog/applications/a-riverhog-event-relay/src/a\_riverhog\_event\_relay/cli.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-event-relay/src/a_riverhog_event_relay/cli.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-event-relay/commands/state/allow_abbrev`
- `/external_contract/cli/a-riverhog-event-relay/commands/state/name`
- `/external_contract/cli/a-riverhog-event-relay/commands/state/parameters`
- `/external_contract/cli/a-riverhog-event-relay/commands/state/subcommand_required`
- `/external_contract/cli/a-riverhog-event-relay/commands/state/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-event-relay/commands/state/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-event-relay/commands/state/name`

<!-- exact-contract-value: 55af5b1d18b0d9ddda33273d987fd0579ef9b07af3a077c79d0accb6c8f17127 -->

```json
"state"
```

### `/external_contract/cli/a-riverhog-event-relay/commands/state/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/a-riverhog-event-relay/commands/state/subcommand_required`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-event-relay/commands/state/terminating_controls`

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

</details>
