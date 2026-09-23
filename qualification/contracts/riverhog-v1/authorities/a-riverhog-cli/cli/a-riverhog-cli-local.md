# a-riverhog-cli local

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-local:d612d735ed -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-3fe349b75e"></a>Parser name: `local`

| Field | Value |
|---|---|
| <a id="s-48d39a10b0"></a>`parameters` | `[]` |
- <a id="s-84be05f573"></a>Subcommand selection: required.
- <a id="s-6a79f59069"></a>Extra arguments at this parser: accepted. Subcommand selection and child parsing still apply.
- <a id="s-58abbf805f"></a>Options after positional arguments at this parser: left as arguments.
- <a id="s-f0aa7c9e1a"></a>Unknown options at this parser: rejected.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-5638b588e1"></a>`help` | <a id="s-a4bdb33d72"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-e167ceb626"></a>`0` | <a id="s-cd7b934b4c"></a>`"noncontractual-framework-help"` | <a id="s-4533a7ed94"></a>`"empty"` |
| <a id="s-def32fac77"></a>`implicit-help` | <a id="s-101b994e68"></a>`{"kind":"empty-invocation"}` | <a id="s-ec9c06d2d1"></a>`2` | <a id="s-e5bde18936"></a>`"empty"` | <a id="s-4a915dca08"></a>`"noncontractual-framework-help"` |

## Governing policies

- <a id="pa-aa461c547f"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/local/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/local/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/local/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/local/name`
- `/external_contract/cli/a-riverhog-cli/commands/local/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/local/subcommand_required`
- `/external_contract/cli/a-riverhog-cli/commands/local/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/local/allow_extra_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/local/allow_interspersed_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/local/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/local/name`

<!-- exact-contract-value: ea17e25c6c7aa6ef9407f976a670e87e37bc7e2a86b7bd4a3305bb16f1ba6052 -->

```json
"local"
```

### `/external_contract/cli/a-riverhog-cli/commands/local/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/a-riverhog-cli/commands/local/subcommand_required`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/local/terminating_controls`

<!-- exact-contract-value: a96bedb6acb408986e7084c1d3216345e293f6c42987bb87a923c18807587f21 -->

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
  },
  {
    "exit_status": 2,
    "id": "implicit-help",
    "stderr": "noncontractual-framework-help",
    "stdout": "empty",
    "trigger": {
      "kind": "empty-invocation"
    }
  }
]
```

</details>
