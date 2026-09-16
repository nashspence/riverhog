# stove0 admission policy

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-admission-policy:2905244e9e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-639a182acc"></a>Parser name: `policy`

| Field | Value |
|---|---|
| <a id="s-2055652af6"></a>`parameters` | `[]` |
- <a id="s-69ff9ec817"></a>Subcommand selection: required.
- <a id="s-6e668bf23c"></a>Extra arguments at this parser: accepted. Subcommand selection and child parsing still apply.
- <a id="s-e128993e6e"></a>Options after positional arguments at this parser: left as arguments.
- <a id="s-b10c65a789"></a>Unknown options at this parser: rejected.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-cc1e89b440"></a>`help` | <a id="s-ea5d8075f1"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-4658a37dd6"></a>`0` | <a id="s-d535c00c41"></a>`"noncontractual-framework-help"` | <a id="s-da782f72c6"></a>`"empty"` |

## Governing policies

- <a id="pa-e63002e29a"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources.md#src-6203ae7d88) — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0/commands/admission/commands/policy/allow_extra_args`
- `/external_contract/cli/stove0/commands/admission/commands/policy/allow_interspersed_args`
- `/external_contract/cli/stove0/commands/admission/commands/policy/ignore_unknown_options`
- `/external_contract/cli/stove0/commands/admission/commands/policy/name`
- `/external_contract/cli/stove0/commands/admission/commands/policy/parameters`
- `/external_contract/cli/stove0/commands/admission/commands/policy/subcommand_required`
- `/external_contract/cli/stove0/commands/admission/commands/policy/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/admission/commands/policy/allow_extra_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0/commands/admission/commands/policy/allow_interspersed_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/admission/commands/policy/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/admission/commands/policy/name`

<!-- exact-contract-value: 17ad92e63c962393c0329c658937d16eccaea13036412a3d1d0a5b6b8f29d738 -->

```json
"policy"
```

### `/external_contract/cli/stove0/commands/admission/commands/policy/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/stove0/commands/admission/commands/policy/subcommand_required`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0/commands/admission/commands/policy/terminating_controls`

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
