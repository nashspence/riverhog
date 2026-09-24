# stove0 departure

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-stove0-cli:stove0-departure:2797ae1cb1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-2b002b1e48"></a>Parser name: `departure`

| Field | Value |
|---|---|
| <a id="s-e59fede416"></a>`parameters` | `[]` |
- <a id="s-26383aa4e2"></a>Subcommand selection: required.
- <a id="s-f60dc4b71f"></a>Extra arguments at this parser: accepted. Subcommand selection and child parsing still apply.
- <a id="s-dd53aef34a"></a>Options after positional arguments at this parser: left as arguments.
- <a id="s-a4180e8772"></a>Unknown options at this parser: rejected.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-9e19d67208"></a>`help` | <a id="s-6edef35e95"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-221e9e1c78"></a>`0` | <a id="s-97cafec7f4"></a>`"noncontractual-framework-help"` | <a id="s-21ed7d92bb"></a>`"empty"` |

## Governing policies

- <a id="pa-5d6e4727b3"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources/authorities.md#src-6203ae7d88) — [some-implementations/stove0/application/client/src/a\_stove0\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/stove0/application/client/src/a_stove0_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/stove0/commands/departure/allow_extra_args`
- `/external_contract/cli/stove0/commands/departure/allow_interspersed_args`
- `/external_contract/cli/stove0/commands/departure/ignore_unknown_options`
- `/external_contract/cli/stove0/commands/departure/name`
- `/external_contract/cli/stove0/commands/departure/parameters`
- `/external_contract/cli/stove0/commands/departure/subcommand_required`
- `/external_contract/cli/stove0/commands/departure/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/departure/allow_extra_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0/commands/departure/allow_interspersed_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/departure/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/departure/name`

<!-- exact-contract-value: 8fd29e272ff19b6ab5b89ed29eab421a0bd4d6be87cae738b488f0e0278d3c30 -->

```json
"departure"
```

### `/external_contract/cli/stove0/commands/departure/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/stove0/commands/departure/subcommand_required`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0/commands/departure/terminating_controls`

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
