"""Closed typed conditions over accepted records; no content or domain inference."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from enum import Enum, StrEnum
from typing import Annotated, Literal, Self

from pydantic import ConfigDict, Field, JsonValue, StrictBool, StringConstraints, model_validator

from stove0_protocol.jcs import canonical_json_bytes
from stove0_protocol.models import LocalName as LocalName
from stove0_protocol.models import SemanticId, Stove0ProtocolModel

Pointer = Annotated[str, StringConstraints(pattern=r"^(?:/(?:[^~/]|~[01])*)*$")]
ViewName = Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9_-]*\.[a-z][a-z0-9_-]*$")]
Quantifier = Literal["any", "every", "none"]


class _Missing(Enum):
    value_absent = "value-absent"


MISSING = _Missing.value_absent


class ConditionModel(Stove0ProtocolModel):
    model_config = ConfigDict(populate_by_name=False, validate_by_name=False)


def pointer_parts(pointer: str) -> tuple[str, ...]:
    return tuple(part.replace("~1", "/").replace("~0", "~") for part in pointer.split("/")[1:])


def read_pointer(value: JsonValue, pointer: str) -> JsonValue | Literal[_Missing.value_absent]:
    for part in pointer_parts(pointer):
        if isinstance(value, dict):
            if part not in value:
                return MISSING
            value = value[part]
        elif (
            isinstance(value, list)
            and part.isascii()
            and part.isdecimal()
            and (part == "0" or not part.startswith("0"))
        ):
            index = int(part)
            if index >= len(value):
                return MISSING
            value = value[index]
        else:
            return MISSING
    return value


class RecordTest(ConditionModel):
    path: Pointer
    op: Literal["eq", "ne", "in", "contains", "exists"]
    value: JsonValue

    @model_validator(mode="after")
    def operand_type(self) -> Self:
        canonical_json_bytes(self.value)
        if self.op == "exists" and type(self.value) is not bool:
            raise ValueError("exists requires a Boolean expected value")
        if self.op == "in" and (not isinstance(self.value, list) or not self.value):
            raise ValueError("in requires a nonempty literal array")
        return self


class RowAll(ConditionModel):
    all: tuple[RowPredicate, ...] = Field(min_length=1)


class RowAny(ConditionModel):
    any: tuple[RowPredicate, ...] = Field(min_length=1)


class RowNot(ConditionModel):
    negated: RowPredicate = Field(alias="not")


class RowTest(ConditionModel):
    test: RecordTest


class ArrayQuantification(ConditionModel):
    path: Pointer
    quantifier: Quantifier
    where: RowPredicate


class RowItems(ConditionModel):
    items: ArrayQuantification


type RowPredicate = StrictBool | RowAll | RowAny | RowNot | RowTest | RowItems


class PredicateAll(ConditionModel):
    all: tuple[Predicate, ...] = Field(min_length=1)


class PredicateAny(ConditionModel):
    any: tuple[Predicate, ...] = Field(min_length=1)


class PredicateNot(ConditionModel):
    negated: Predicate = Field(alias="not")


class FactsQuantification(ConditionModel):
    view: ViewName
    scope: Literal["input", "candidate", "self"]
    roles: tuple[SemanticId, ...] | None = Field(default=None, min_length=1)
    quantifier: Quantifier
    inspect: Literal["records", "status"] = "records"
    where: RowPredicate

    @model_validator(mode="after")
    def unique_roles(self) -> Self:
        if self.roles is not None and len(self.roles) != len(set(self.roles)):
            raise ValueError("fact role filters must be unique")
        return self


class PredicateFacts(ConditionModel):
    facts: FactsQuantification


type Predicate = StrictBool | PredicateAll | PredicateAny | PredicateNot | PredicateFacts


class Truth(StrEnum):
    TRUE = "true"
    FALSE = "false"
    INDETERMINATE = "indeterminate"


def negate(value: Truth) -> Truth:
    if value == Truth.INDETERMINATE:
        return value
    return Truth.FALSE if value == Truth.TRUE else Truth.TRUE


def quantify(kind: Quantifier, values: Iterable[Truth]) -> Truth:
    seen = positive = negative = unresolved = False
    for value in values:
        seen = True
        positive |= value == Truth.TRUE
        negative |= value == Truth.FALSE
        unresolved |= value == Truth.INDETERMINATE
    if kind == "every":
        if not seen or negative:
            return Truth.FALSE
        return Truth.INDETERMINATE if unresolved else Truth.TRUE
    if positive:
        result = Truth.TRUE
    else:
        result = Truth.INDETERMINATE if unresolved else Truth.FALSE
    return negate(result) if kind == "none" else result


def json_equal(left: JsonValue, right: JsonValue) -> bool:
    return canonical_json_bytes(left) == canonical_json_bytes(right)


def evaluate_test(test: RecordTest, record: JsonValue) -> Truth:
    actual = read_pointer(record, test.path)
    if test.op == "exists":
        answer = (actual is not MISSING) == test.value
    elif actual is MISSING:
        return Truth.INDETERMINATE
    elif test.op in {"eq", "ne"}:
        answer = json_equal(actual, test.value)
        if test.op == "ne":
            answer = not answer
    elif test.op == "in":
        assert isinstance(test.value, list)
        answer = any(json_equal(actual, item) for item in test.value)
    elif isinstance(actual, list):
        answer = any(json_equal(item, test.value) for item in actual)
    elif isinstance(actual, str) and isinstance(test.value, str):
        answer = test.value in actual
    else:
        raise ValueError("contains operands contradict their declared types")
    return Truth.TRUE if answer else Truth.FALSE


def evaluate_row(predicate: RowPredicate, record: JsonValue) -> Truth:
    if isinstance(predicate, bool):
        return Truth.TRUE if predicate else Truth.FALSE
    if isinstance(predicate, RowAll):
        return quantify("every", (evaluate_row(item, record) for item in predicate.all))
    if isinstance(predicate, RowAny):
        return quantify("any", (evaluate_row(item, record) for item in predicate.any))
    if isinstance(predicate, RowNot):
        return negate(evaluate_row(predicate.negated, record))
    if isinstance(predicate, RowTest):
        return evaluate_test(predicate.test, record)
    array = read_pointer(record, predicate.items.path)
    if array is MISSING:
        return Truth.INDETERMINATE
    if not isinstance(array, list):
        raise ValueError("nested items predicate requires its declared array")
    return quantify(
        predicate.items.quantifier, (evaluate_row(predicate.items.where, item) for item in array)
    )


def evaluate_predicate(
    predicate: Predicate, facts: Callable[[FactsQuantification], Truth]
) -> Truth:
    if isinstance(predicate, bool):
        return Truth.TRUE if predicate else Truth.FALSE
    if isinstance(predicate, PredicateAll):
        return quantify("every", (evaluate_predicate(item, facts) for item in predicate.all))
    if isinstance(predicate, PredicateAny):
        return quantify("any", (evaluate_predicate(item, facts) for item in predicate.any))
    if isinstance(predicate, PredicateNot):
        return negate(evaluate_predicate(predicate.negated, facts))
    return facts(predicate.facts)


for _model in (
    RowAll,
    RowAny,
    RowNot,
    RowItems,
    ArrayQuantification,
    PredicateAll,
    PredicateAny,
    PredicateNot,
    PredicateFacts,
    FactsQuantification,
):
    _model.model_rebuild()


def facts_predicates(condition: Predicate) -> Iterator[FactsQuantification]:
    pending = [condition]
    while pending:
        node = pending.pop()
        if isinstance(node, PredicateAll):
            pending.extend(reversed(node.all))
        elif isinstance(node, PredicateAny):
            pending.extend(reversed(node.any))
        elif isinstance(node, PredicateNot):
            pending.append(node.negated)
        elif isinstance(node, PredicateFacts):
            yield node.facts
