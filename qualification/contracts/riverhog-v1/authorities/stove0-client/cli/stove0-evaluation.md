# stove0 evaluation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-evaluation:2f073559cf -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-1b69af7f1f"></a>Parser name: `evaluation`

| Field | Value |
|---|---|
| <a id="s-c1ec27d6d0"></a>`parameters` | `[]` |
- <a id="s-a044e3350a"></a>Subcommand selection: required.
- <a id="s-78047be058"></a>Extra arguments at this parser: accepted. Subcommand selection and child parsing still apply.
- <a id="s-99d4082a08"></a>Options after positional arguments at this parser: left as arguments.
- <a id="s-887490b749"></a>Unknown options at this parser: rejected.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-5810491661"></a>`help` | <a id="s-e2bea69ca3"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-2b78648fcd"></a>`0` | <a id="s-7416387574"></a>`"noncontractual-framework-help"` | <a id="s-4549840ed8"></a>`"empty"` |

## Governing policies

- <a id="pa-5df12fe3a7"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources/authorities.md#src-6203ae7d88) — [reference/stove0/application/client/src/stove0\_cli/main.py::&lt;module&gt;](../../../../../../reference/stove0/application/client/src/stove0_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/stove0/commands/evaluation/allow_extra_args`
- `/external_contract/cli/stove0/commands/evaluation/allow_interspersed_args`
- `/external_contract/cli/stove0/commands/evaluation/ignore_unknown_options`
- `/external_contract/cli/stove0/commands/evaluation/name`
- `/external_contract/cli/stove0/commands/evaluation/parameters`
- `/external_contract/cli/stove0/commands/evaluation/subcommand_required`
- `/external_contract/cli/stove0/commands/evaluation/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/evaluation/allow_extra_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0/commands/evaluation/allow_interspersed_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/evaluation/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/evaluation/name`

<!-- exact-contract-value: 825cff9607c815f556865135b4bd35491d8509f05a986a6faba376dec4ab503e -->

```json
"evaluation"
```

### `/external_contract/cli/stove0/commands/evaluation/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/stove0/commands/evaluation/subcommand_required`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0/commands/evaluation/terminating_controls`

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
