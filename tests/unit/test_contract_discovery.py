from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

if str(Path(__file__).resolve().parents[2] / "scripts") not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

from contract_discovery import (  # noqa: E402
    discover_configuration_documents,
    discover_environment_reads,
)


@dataclass(frozen=True)
class Project:
    name: str
    path: str


def _source(tmp_path: Path, value: str) -> tuple[Path, list[Project]]:
    root = tmp_path / "repo"
    path = root / "component/src/example/config.py"
    path.parent.mkdir(parents=True)
    path.write_text(value, encoding="utf-8")
    return root, [Project(name="example", path="component")]


def test_configuration_detector_resolves_composed_names_and_helper_arguments(
    tmp_path: Path,
) -> None:
    root, projects = _source(
        tmp_path,
        """
import os

PREFIX = "RIVERHOG_EXAMPLE_"

def read(suffix: str) -> str | None:
    return os.getenv(f"{PREFIX}{suffix}", "fallback")

def boot() -> str | None:
    return read("TOKEN")
""",
    )

    detections = discover_environment_reads(root, projects)

    assert len(detections) == 1
    assert detections[0]["resolved_names"] == ["RIVERHOG_EXAMPLE_TOKEN"]
    assert detections[0]["default_expression"] == "'fallback'"


def test_configuration_detector_keeps_an_unresolved_direct_read_visible(
    tmp_path: Path,
) -> None:
    root, projects = _source(
        tmp_path,
        """
import os

def runtime_name() -> str:
    raise NotImplementedError

def read() -> str:
    return os.environ[runtime_name()]
""",
    )

    detections = discover_environment_reads(root, projects)

    assert len(detections) == 1
    assert detections[0]["resolved_names"] == []
    assert detections[0]["name_expression"] == "runtime_name()"


def test_injected_mapping_detection_rejects_unrelated_dictionary_fields(
    tmp_path: Path,
) -> None:
    root, projects = _source(
        tmp_path,
        """
def read(values: dict[str, str]) -> tuple[str | None, str | None]:
    return values.get("RIVERHOG_EXAMPLE_TOKEN"), values.get("UBR")
""",
    )

    detections = discover_environment_reads(root, projects)

    assert [item["resolved_names"] for item in detections] == [["RIVERHOG_EXAMPLE_TOKEN"]]


def test_configuration_detector_does_not_merge_same_named_helpers_across_modules(
    tmp_path: Path,
) -> None:
    root, projects = _source(
        tmp_path,
        """
def _setting(values: dict[str, str], name: str) -> str | None:
    return values.get(name)

def read(values: dict[str, str]) -> str | None:
    return _setting(values, "RIVERHOG_FIRST")
""",
    )
    second = root / "component/src/example/other.py"
    second.write_text(
        """
def _setting(values: dict[str, str], name: str) -> str | None:
    return values.get(name)

def read(values: dict[str, str]) -> str | None:
    return _setting(values, "RIVERHOG_SECOND")
""",
        encoding="utf-8",
    )

    detections = discover_environment_reads(root, projects)

    assert sorted(item["resolved_names"] for item in detections) == [
        ["RIVERHOG_FIRST"],
        ["RIVERHOG_SECOND"],
    ]


def test_configuration_detector_resolves_imported_helper_and_constructor_arguments(
    tmp_path: Path,
) -> None:
    root, projects = _source(
        tmp_path,
        """
import os

class Client:
    def __init__(self, token_name: str) -> None:
        self.token = os.getenv(token_name)
""",
    )
    caller = root / "component/src/example/caller.py"
    caller.write_text(
        """
from .config import Client

CLIENT = Client("RIVERHOG_IMPORTED_TOKEN")
""",
        encoding="utf-8",
    )

    detections = discover_environment_reads(root, projects)

    assert [item["resolved_names"] for item in detections] == [["RIVERHOG_IMPORTED_TOKEN"]]


def test_configuration_document_detector_follows_runtime_validation_authority(
    tmp_path: Path,
) -> None:
    root, projects = _source(
        tmp_path,
        """
class ExampleConfig:
    @classmethod
    def model_validate(cls, value: object):
        return cls()

def load_config(value: object) -> ExampleConfig:
    return ExampleConfig.model_validate(value)
""",
    )

    detections = discover_configuration_documents(root, projects)

    assert len(detections) == 1
    assert detections[0]["authority_module"] == "example.config"
    assert detections[0]["authority_qualname"] == "ExampleConfig"
    assert detections[0]["input_shape"] == "model"


def test_configuration_document_detector_keeps_unknown_authority_visible(
    tmp_path: Path,
) -> None:
    root, projects = _source(
        tmp_path,
        """
def load_config(model: type, value: object):
    return model.model_validate(value)
""",
    )

    detections = discover_configuration_documents(root, projects)

    assert len(detections) == 1
    assert detections[0]["authority_module"] is None
    assert detections[0]["authority_qualname"] is None


def test_configuration_document_detector_resolves_imported_authority(
    tmp_path: Path,
) -> None:
    root, projects = _source(
        tmp_path,
        """
class SharedConfig:
    @classmethod
    def model_validate(cls, value: object):
        return cls()
""",
    )
    consumer = root / "component/src/example/loader.py"
    consumer.write_text(
        """
from .config import SharedConfig

def load(value: object) -> SharedConfig:
    return SharedConfig.model_validate(value)
""",
        encoding="utf-8",
    )

    detections = discover_configuration_documents(root, projects)

    assert len(detections) == 1
    assert detections[0]["authority_module"] == "example.config"
    assert detections[0]["authority_qualname"] == "SharedConfig"
