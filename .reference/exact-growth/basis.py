# SPDX-License-Identifier: Apache-2.0
"""Reference-only arithmetic bases for #969's required asymptotic classes.

These are not numerical product limits, a declaration format, a runtime monitor,
or a second normative registry. Coefficients are normalized to one solely to
challenge dimensions and composition; a returned number is NOT a measured bound.
Actual declarations belong to riverhog_protocol.growth and the operation owners.
"""
from __future__ import annotations

from dataclasses import dataclass


def natural(value: int) -> int:
    if type(value) is not int or value < 0:
        raise ValueError("dimension must be a nonnegative integer")
    return value


def ell(population: int) -> int:
    """Exact ceil(log2(2+population)), without floating-point rounding."""
    return (natural(population) + 1).bit_length()


@dataclass(frozen=True)
class Basis:
    byte_visits: int
    index_operations: int
    key_comparisons: int
    comparison_octets: int

    def __post_init__(self) -> None:
        for value in (self.byte_visits, self.index_operations,
                      self.key_comparisons, self.comparison_octets):
            natural(value)


def vector(byte_visits: int, probes: int, population: int, key_width: int) -> Basis:
    comparisons = natural(probes) * ell(population)
    return Basis(natural(byte_visits), probes, comparisons,
                 comparisons * natural(key_width))


def edit(*, authored_octets: int, tag: bool, path: int, node_octets: int,
         control_octets: int, grants: int, grant_octets: int, scheduled_copies: int,
         population: int, key_width: int) -> Basis:
    if type(tag) is not bool:
        raise ValueError("tag selector must be a bool")
    for value in (authored_octets, path, node_octets, control_octets, grants,
                  grant_octets, scheduled_copies, population, key_width):
        natural(value)
    d = path if tag else 0
    return vector(authored_octets + control_octets * (1 + scheduled_copies)
                  + d * node_octets + grant_octets,
                  1 + d + grants + scheduled_copies, population, key_width)


def attempts(*, request_octets: int, attempt_count: int, tag_attempts: int,
             path: int, node_octets: int, control_octets: int,
             total_grants: int, total_grant_octets: int, copy_scheduling_visits: int,
             population: int, key_width: int) -> Basis:
    for value in (request_octets, attempt_count, tag_attempts, path, node_octets,
                  control_octets, total_grants, total_grant_octets,
                  copy_scheduling_visits, population, key_width):
        natural(value)
    if tag_attempts > attempt_count:
        raise ValueError("tag attempts are a subset of all attempts")
    return vector(request_octets + control_octets * (attempt_count + copy_scheduling_visits)
                  + path * node_octets * tag_attempts + total_grant_octets,
                  attempt_count + path * tag_attempts + total_grants + copy_scheduling_visits,
                  population, key_width)


def retained(*, initial_tags: int, initial_octets: int, changes: int, tag_changes: int,
             authored_octets: int, path: int, node_octets: int,
             control_octets: int) -> tuple[int, int]:
    for value in (initial_tags, initial_octets, changes, tag_changes, authored_octets,
                  path, node_octets, control_octets):
        natural(value)
    if tag_changes > changes:
        raise ValueError("tag changes are a subset of changes")
    objects = 1 + initial_tags + changes + path * tag_changes
    octets = (initial_octets + authored_octets
              + control_octets * (1 + initial_tags + changes)
              + path * node_octets * tag_changes)
    return objects, octets


def recovery(*, selected_octets: int, objects: int, edges: int, selections: int,
             changes: int, tag_changes: int, path: int, node_octets: int,
             output_octets: int, population: int, key_width: int) -> Basis:
    for value in (selected_octets, objects, edges, selections, changes, tag_changes,
                  path, node_octets, output_octets, population, key_width):
        natural(value)
    if tag_changes > changes:
        raise ValueError("tag changes are a subset of changes")
    return vector(selected_octets + path * node_octets * tag_changes + output_octets,
                  1 + objects + edges + selections + changes + path * tag_changes,
                  population, key_width)


def restart(*, interruptions: int, byte_budget: int, visit_budget: int,
            index_budget: int, population: int, key_width: int) -> tuple[Basis, int]:
    for value in (interruptions, byte_budget, visit_budget, index_budget,
                  population, key_width):
        natural(value)
    return (vector(interruptions * byte_budget, interruptions * (1 + index_budget),
                   population, key_width), interruptions * visit_budget)
