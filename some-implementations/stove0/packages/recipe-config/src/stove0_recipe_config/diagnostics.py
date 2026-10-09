"""Semantic source pointers belong to the single compiler; file locations are auxiliary."""

from collections.abc import Callable, Iterator
from contextlib import contextmanager


def source_pointer(*parts: str) -> str:
    return "".join("/" + part.replace("~", "~0").replace("/", "~1") for part in parts)


class RecipeCompileError(ValueError):
    def __init__(self, pointer: str, message: str):
        self.pointer, self.message = pointer, message
        super().__init__(f"{pointer or '/'}: {message}")


@contextmanager
def compile_at(pointer: str) -> Iterator[None]:
    try:
        yield
    except RecipeCompileError as error:
        raise RecipeCompileError(pointer + error.pointer, error.message) from error
    except KeyError as error:
        raise RecipeCompileError(pointer, f"unknown source name: {error.args[0]}") from error
    except ValueError as error:
        raise RecipeCompileError(pointer, str(error)) from error


def compile_value[**P, T](
    pointer: str, operation: Callable[P, T], *args: P.args, **kwargs: P.kwargs
) -> T:
    with compile_at(pointer):
        return operation(*args, **kwargs)
