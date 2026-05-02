from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import yaml

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_STATECHARTS_CONTRACT = ROOT / "contracts" / "operator" / "statecharts.yaml"
DEFAULT_STATECHARTS_SCHEMA = ROOT / "contracts" / "operator" / "statecharts.schema.json"


class StatechartCatalogError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class OperatorDecision:
    statechart: str
    state: str
    matched_guard: str | None = None


@dataclass(frozen=True, slots=True)
class OperatorView:
    statechart: str
    state: str
    copy_ref: str
    text: str


@dataclass(frozen=True, slots=True)
class Handoff:
    from_statechart: str
    from_state: str
    label: str
    target_statechart: str
    target_state: str
    event: str | None = None


def _mapping(value: object, *, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise StatechartCatalogError(f"{label} must be a mapping")
    return cast(Mapping[str, Any], value)


def _sequence(value: object, *, label: str) -> Sequence[Any]:
    if isinstance(value, str) or not isinstance(value, Sequence):
        raise StatechartCatalogError(f"{label} must be a sequence")
    return value


def _load_yaml_mapping(path: Path) -> Mapping[str, Any]:
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise StatechartCatalogError(f"{path} is not valid YAML") from exc
    return _mapping(payload, label=str(path))


class StatechartCatalog:
    def __init__(
        self,
        *,
        statecharts: Mapping[str, Mapping[str, Any]],
        handoffs: Sequence[Handoff] = (),
    ) -> None:
        self._statecharts = dict(statecharts)
        self._handoffs = tuple(handoffs)

    @classmethod
    def load(
        cls,
        contract_path: Path = DEFAULT_STATECHARTS_CONTRACT,
        *,
        schema_path: Path | None = None,
    ) -> StatechartCatalog:
        contract = _load_yaml_mapping(contract_path)
        if schema_path is not None:
            validate_statechart_contract(contract, schema_path=schema_path)
        if contract.get("version") != 1:
            raise StatechartCatalogError("statechart contract version must be 1")
        raw_statecharts = _mapping(contract.get("statecharts"), label="statecharts")
        raw_handoffs = contract.get("handoffs", ())
        if raw_handoffs is None:
            raw_handoffs = ()
        handoffs = tuple(
            _handoff_from_mapping(_mapping(item, label="handoffs[]"))
            for item in _sequence(raw_handoffs, label="handoffs")
        )
        statecharts = {
            str(name): _mapping(statechart, label=f"statecharts.{name}")
            for name, statechart in raw_statecharts.items()
        }
        return cls(statecharts=statecharts, handoffs=handoffs)

    @property
    def statecharts(self) -> Mapping[str, Mapping[str, Any]]:
        return self._statecharts

    @property
    def handoffs(self) -> tuple[Handoff, ...]:
        return self._handoffs

    def require_statechart(self, name: str) -> Mapping[str, Any]:
        try:
            return self._statecharts[name]
        except KeyError as exc:
            raise StatechartCatalogError(f"unknown statechart: {name}") from exc

    def require_state(self, statechart: str, state: str) -> Mapping[str, Any]:
        statechart_payload = self.require_statechart(statechart)
        states = _mapping(statechart_payload.get("states"), label=f"{statechart}.states")
        try:
            return _mapping(states[state], label=f"{statechart}.{state}")
        except KeyError as exc:
            raise StatechartCatalogError(f"unknown state: {statechart}.{state}") from exc

    def view_for(self, statechart: str, state: str) -> str | None:
        state_payload = self.require_state(statechart, state)
        view = state_payload.get("view")
        return str(view) if view else None

    def require_view(self, statechart: str, state: str, view: str) -> None:
        actual = self.view_for(statechart, state)
        if actual != view:
            raise StatechartCatalogError(
                f"{statechart}.{state} uses view {actual!r}, expected {view!r}"
            )

    def state_for_guard(self, statechart: str, state: str, guard: str) -> str:
        state_payload = self.require_state(statechart, state)
        for transition in _sequence(
            state_payload.get("transitions", ()),
            label=f"{statechart}.{state}.transitions",
        ):
            transition_payload = _mapping(
                transition,
                label=f"{statechart}.{state}.transitions[]",
            )
            if transition_payload.get("guard") == guard:
                return str(transition_payload["target"])
        raise StatechartCatalogError(f"{statechart}.{state} has no guard {guard!r}")

    def state_for_event(self, statechart: str, state: str, event: str) -> str:
        state_payload = self.require_state(statechart, state)
        for transition in _sequence(
            state_payload.get("transitions", ()),
            label=f"{statechart}.{state}.transitions",
        ):
            transition_payload = _mapping(
                transition,
                label=f"{statechart}.{state}.transitions[]",
            )
            if transition_payload.get("event") == event:
                return str(transition_payload["target"])
        raise StatechartCatalogError(f"{statechart}.{state} has no event {event!r}")

    def transition_targets(self, statechart: str, state: str) -> tuple[str, ...]:
        state_payload = self.require_state(statechart, state)
        transitions = state_payload.get("transitions", ())
        if not transitions:
            return ()
        return tuple(
            str(_mapping(transition, label=f"{statechart}.{state}.transitions[]")["target"])
            for transition in _sequence(
                transitions,
                label=f"{statechart}.{state}.transitions",
            )
        )

    def handoffs_from(self, statechart: str, state: str) -> tuple[Handoff, ...]:
        self.require_state(statechart, state)
        return tuple(
            handoff
            for handoff in self._handoffs
            if handoff.from_statechart == statechart and handoff.from_state == state
        )

    def decision(
        self,
        statechart: str,
        state: str,
        *,
        matched_guard: str | None = None,
    ) -> OperatorDecision:
        self.require_state(statechart, state)
        return OperatorDecision(
            statechart=statechart,
            state=state,
            matched_guard=matched_guard,
        )

    def operator_view(
        self,
        statechart: str,
        state: str,
        *,
        text: str,
    ) -> OperatorView:
        copy_ref = self.view_for(statechart, state)
        if copy_ref is None:
            raise StatechartCatalogError(f"{statechart}.{state} has no operator view")
        return OperatorView(
            statechart=statechart,
            state=state,
            copy_ref=copy_ref,
            text=text,
        )


def load_default_statechart_catalog(*, validate_schema: bool = False) -> StatechartCatalog:
    schema_path = DEFAULT_STATECHARTS_SCHEMA if validate_schema else None
    return StatechartCatalog.load(schema_path=schema_path)


def validate_statechart_contract(
    contract: Mapping[str, Any],
    *,
    schema_path: Path = DEFAULT_STATECHARTS_SCHEMA,
) -> None:
    from jsonschema import Draft202012Validator  # type: ignore[import-untyped]

    schema = _load_yaml_mapping(schema_path)
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(schema)
    errors = sorted(validator.iter_errors(contract), key=lambda error: error.json_path)
    if errors:
        details = "; ".join(f"{error.json_path}: {error.message}" for error in errors)
        raise StatechartCatalogError(details)


def _handoff_from_mapping(payload: Mapping[str, Any]) -> Handoff:
    source = _mapping(payload.get("from"), label="handoffs[].from")
    target = _mapping(payload.get("target"), label="handoffs[].target")
    event = payload.get("event")
    return Handoff(
        from_statechart=str(source["statechart"]),
        from_state=str(source["state"]),
        label=str(payload["label"]),
        target_statechart=str(target["statechart"]),
        target_state=str(target["state"]),
        event=str(event) if event else None,
    )
