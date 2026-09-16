# stove0 admission

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-admission:1a4091d7d0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-696fd03480"></a>Parser name: `admission`
- <a id="s-d42a904cd7"></a>Subcommand selection: required.
- <a id="s-e0c7bc7e83"></a>Extra arguments at this parser: accepted. Subcommand selection and child parsing still apply.
- <a id="s-ede70acab8"></a>Options after positional arguments at this parser: left as arguments.
- <a id="s-d66213a267"></a>Unknown options at this parser: rejected.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-0fb43d6481"></a>`help` | <a id="s-8b91bee5a5"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-2ba0c71f08"></a>`0` | <a id="s-dcc00822fa"></a>`"noncontractual-framework-help"` | <a id="s-039502230a"></a>`"empty"` |

## Governing policies

- <a id="pa-c5f0ab24d8"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources.md#src-6203ae7d88) — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0/commands/admission/allow_extra_args`
- `/external_contract/cli/stove0/commands/admission/allow_interspersed_args`
- `/external_contract/cli/stove0/commands/admission/ignore_unknown_options`
- `/external_contract/cli/stove0/commands/admission/name`
- `/external_contract/cli/stove0/commands/admission/parameters`
- `/external_contract/cli/stove0/commands/admission/subcommand_required`
- `/external_contract/cli/stove0/commands/admission/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/admission/allow_extra_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0/commands/admission/allow_interspersed_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/admission/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/admission/name`

<!-- exact-contract-value: 5f37bb96609f12de3e4fed376d2b235895dc8129f9997d52b775bbb89a56132e -->

```json
"admission"
```

### `/external_contract/cli/stove0/commands/admission/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/stove0/commands/admission/subcommand_required`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0/commands/admission/terminating_controls`

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
