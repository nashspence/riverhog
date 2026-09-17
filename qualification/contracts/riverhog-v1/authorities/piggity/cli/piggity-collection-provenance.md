# piggity collection provenance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-collection-provenance:d175d5410d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-1247d0a244"></a>Parser name: `provenance`

| Field | Value |
|---|---|
| <a id="s-c25cbe833d"></a>`parameters` | `[]` |
- <a id="s-b1a9d91897"></a>Subcommand selection: required.
- <a id="s-39745146d4"></a>Extra arguments at this parser: accepted. Subcommand selection and child parsing still apply.
- <a id="s-24fcb8a0ac"></a>Options after positional arguments at this parser: left as arguments.
- <a id="s-fb48963308"></a>Unknown options at this parser: rejected.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-949d84f78f"></a>`help` | <a id="s-5a57731387"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-b1a78589ee"></a>`0` | <a id="s-5e761b7154"></a>`"noncontractual-framework-help"` | <a id="s-076ef710b7"></a>`"empty"` |

## Governing policies

- <a id="pa-9b3a252c39"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — [reference/riverhog/applications/piggity/src/piggity/main.py::&lt;module&gt;](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py)
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/piggity/commands/collection/commands/provenance/allow_extra_args`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/allow_interspersed_args`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/ignore_unknown_options`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/name`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/parameters`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/subcommand_required`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/collection/commands/provenance/allow_extra_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/allow_interspersed_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/name`

<!-- exact-contract-value: 177887b1fb536becb2829efbdfcb7f7b2c801f57dd46da7319fa7d5bf9838bb1 -->

```json
"provenance"
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/subcommand_required`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/terminating_controls`

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
