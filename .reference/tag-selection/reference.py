"""Non-authoritative selector/continuation witness; no production imports or state store."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Mapping

FIELDS = ("tags_all", "tags_any", "tags_none")
CLAUSE_MAX = 256


@dataclass(frozen=True)
class Selector:
    tags_all: tuple[str, ...] = ()
    tags_any: tuple[str, ...] = ()
    tags_none: tuple[str, ...] = ()

    @classmethod
    def parse(cls, value: object, validate_tag: Callable[[str], str]) -> Selector:
        if type(value) is not dict or set(value) - set(FIELDS):
            raise ValueError("selector must be a closed three-clause object")
        groups = {}
        for name in FIELDS:
            items = value.get(name, [])
            if type(items) not in (list, tuple) or len(items) > CLAUSE_MAX:
                raise ValueError("clause must be an array of at most 256 tags")
            tags = tuple(validate_tag(item) for item in items)
            if len(tags) != len(set(tags)):
                raise ValueError("duplicates within a clause are invalid")
            groups[name] = tags
        return cls(**groups)

    def document(self) -> dict[str, list[str]]:
        return {name: list(getattr(self, name)) for name in FIELDS}


@dataclass(frozen=True)
class Binding:
    """Opaque caller-owned fences; this witness does not authenticate them."""

    source: str
    view: str
    policy: str
    generation: str
    descriptor_authority: str
    collection_id: str
    tag_revision: int
    tag_set_identity: str


def exact_truth(binding: Binding, tag: str, response: object) -> bool:
    if not isinstance(response, Mapping):
        raise ValueError("missing membership response")
    expected = {
        "collection_id": binding.collection_id,
        "revision": binding.tag_revision,
        "tag_set_identity": binding.tag_set_identity,
        "tag": tag,
    }
    for name, value in expected.items():
        actual = response.get(name)
        if type(actual) is not type(value) or actual != value:
            raise ValueError("membership response differs from exact authority")
    present = response.get("present")
    if type(present) is not bool:
        raise ValueError("membership response is not Boolean evidence")
    return present


@dataclass
class Evaluation:
    """One example, not a required SDK signature or persisted product format.

    A step makes at most one remote probe. Consumers own durable row/checkpoint
    acceptance, real authorization, concurrency, budgets, and catalog cursors.
    """

    selector: Selector
    binding: Binding
    phase: int = 0
    offset: int = 0
    evidence: dict[str, bool] = field(default_factory=dict)
    result: bool | None = None

    def step(
        self,
        current_binding: Binding,
        probe: Callable[[Binding, str], object],
    ) -> bool | None:
        if current_binding != self.binding:
            raise ValueError("stale evaluation binding")
        contacted = False
        # Internal order is not a serialization or identity rule.
        names = ("tags_none", "tags_all", "tags_any")
        while self.result is None:
            if self.phase == len(names):
                self.result = True
                break
            name = names[self.phase]
            tags = getattr(self.selector, name)
            if self.offset == len(tags):
                if name == "tags_any" and tags:
                    self.result = False
                    break
                self.phase += 1
                self.offset = 0
                continue
            tag = tags[self.offset]
            if tag not in self.evidence:
                if contacted:
                    return None
                # Exceptions do not become False or consume this probe position.
                answer = exact_truth(self.binding, tag, probe(self.binding, tag))
                self.evidence[tag] = answer
                contacted = True
            present = self.evidence[tag]
            self.offset += 1
            if (name == "tags_none" and present) or (name == "tags_all" and not present):
                self.result = False
            elif name == "tags_any" and present:
                self.result = True
        return self.result
