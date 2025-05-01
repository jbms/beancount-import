from bs4.element import Tag, PageElement, _FindMethodName, NavigableString

from typing import Optional, Any, Union, cast, Pattern, Callable, Iterable
from bs4._typing import (
    _OneElement,
    _StrainableAttribute,
    _StrainableAttributes,
    _StrainableString,
)
def require_find(
    tag: Tag,
    name: _FindMethodName = None,
    attrs: _StrainableAttributes = {},
    recursive: bool = True,
    string: Optional[_StrainableString] = None,
    **kwargs: _StrainableAttribute,
) -> _OneElement:

# def require_find(
#     tag: Tag,
#     name: _FindMethodName = None,
#     attrs: Optional[dict[str, Any]] = None,
#     recursive: bool = True,
#     text: Any = None,
#     limit: str | bool | bytes | Pattern[str] | Callable[[str], bool] | Callable[[Tag], bool] | None | Iterable[str | bool | bytes | Pattern[str] | Callable[[str], bool] | Callable[[Tag], bool] | None] = None,
#     **kwargs: Any
# ) -> Tag:
    """
    Monkey-patched method for Tag that behaves like find() but raises if element is not found.
    Assumes the result is always a Tag (not NavigableString).
    """
    result = tag.find(
        name=name,
        attrs=attrs,
        recursive=recursive,
        string=string,
        **kwargs,
    )
    if result is None or not isinstance(result, Tag):
        raise ValueError(f"Element not found or not a Tag: {name}, {attrs}, {kwargs}")
    return cast(Tag, result)


# Monkey-patch the method into bs4.element.Tag
#Tag.require_find = require_find  # type: ignore # type: ignore[assignment]
# PageElement.require_find = require_find   # type: ignore
