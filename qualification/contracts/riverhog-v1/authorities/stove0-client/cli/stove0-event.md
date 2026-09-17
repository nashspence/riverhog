# stove0 event

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-event:0d68bf01f6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-c86560ef4b"></a>Parser name: `event`

| Field | Value |
|---|---|
| <a id="s-d508c488c4"></a>`parameters` | `[]` |
- <a id="s-454394cf06"></a>Subcommand selection: required.
- <a id="s-96e1d122bc"></a>Extra arguments at this parser: accepted. Subcommand selection and child parsing still apply.
- <a id="s-082317c117"></a>Options after positional arguments at this parser: left as arguments.
- <a id="s-8e2e154374"></a>Unknown options at this parser: rejected.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-a02f96c82c"></a>`help` | <a id="s-930ce87c58"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-eea18fcaaf"></a>`0` | <a id="s-c8e4507b2a"></a>`"noncontractual-framework-help"` | <a id="s-157703110a"></a>`"empty"` |

## Governing policies

- <a id="pa-f0f9b48556"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources/authorities.md#src-6203ae7d88) — [reference/stove0/application/client/src/stove0\_cli/main.py::&lt;module&gt;](../../../../../../reference/stove0/application/client/src/stove0_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/stove0/commands/event/allow_extra_args`
- `/external_contract/cli/stove0/commands/event/allow_interspersed_args`
- `/external_contract/cli/stove0/commands/event/ignore_unknown_options`
- `/external_contract/cli/stove0/commands/event/name`
- `/external_contract/cli/stove0/commands/event/parameters`
- `/external_contract/cli/stove0/commands/event/subcommand_required`
- `/external_contract/cli/stove0/commands/event/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/event/allow_extra_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0/commands/event/allow_interspersed_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/event/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/event/name`

<!-- exact-contract-value: 8f6e29cfb2f0df76f9f90f221edf3a7b4cfc6ba2e409bb0a78a2c031afd78580 -->

```json
"event"
```

### `/external_contract/cli/stove0/commands/event/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/stove0/commands/event/subcommand_required`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0/commands/event/terminating_controls`

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
