# a-riverhog-cli

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli:8ff629ccce -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-e5853bc1b1"></a>Parser name: `a-riverhog-cli`

| Field | Value |
|---|---|
| <a id="s-2e6e54c664"></a>`parameters` | `[]` |
- <a id="s-c4ab44874d"></a>Subcommand selection: required.
- <a id="s-5988c994be"></a>Extra arguments at this parser: accepted. Subcommand selection and child parsing still apply.
- <a id="s-c6e96718d2"></a>Options after positional arguments at this parser: left as arguments.
- <a id="s-5cad349a83"></a>Unknown options at this parser: rejected.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-396d40cbbf"></a>`help` | <a id="s-4bad5e49cd"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-c77f7a2fbd"></a>`0` | <a id="s-3d272fab97"></a>`"noncontractual-framework-help"` | <a id="s-b3d33ff8fc"></a>`"empty"` |
| <a id="s-ab2f299d27"></a>`version` | <a id="s-c8587b4519"></a>`{"kind":"option-present","options":["--version"]}` | <a id="s-a50ea557b2"></a>`0` | <a id="s-fa2b58824e"></a>`{"distribution":"a-riverhog-cli","kind":"installed-coordinated-release-version","serialization":"noncontractual"}` | <a id="s-b5f0885963"></a>`"empty"` |

## Governing policies

- <a id="pa-5b8c1aac42"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/name`
- `/external_contract/cli/a-riverhog-cli/parameters`
- `/external_contract/cli/a-riverhog-cli/subcommand_required`
- `/external_contract/cli/a-riverhog-cli/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/allow_extra_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/allow_interspersed_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/name`

<!-- exact-contract-value: 25be532425813c72879bb7587d3ff00f58bb458ff26a67cd655b8280d7822713 -->

```json
"a-riverhog-cli"
```

### `/external_contract/cli/a-riverhog-cli/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/a-riverhog-cli/subcommand_required`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/terminating_controls`

<!-- exact-contract-value: b6425185b89fbd1238883e262762f634719777e188fcc0fccc02c0e419af0304 -->

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
    "exit_status": 0,
    "id": "version",
    "stderr": "empty",
    "stdout": {
      "distribution": "a-riverhog-cli",
      "kind": "installed-coordinated-release-version",
      "serialization": "noncontractual"
    },
    "trigger": {
      "kind": "option-present",
      "options": [
        "--version"
      ]
    }
  }
]
```

</details>
