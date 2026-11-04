from bs4.element import Tag, PageElement, _FindMethodName, NavigableString

from typing import Optional, Any, Union, cast, Pattern, Callable, Iterable
from bs4._typing import (
    _OneElement,
    _StrainableAttribute,
    _StrainableAttributes,
    _StrainableString,
)

#
#
def require_find(
    tag: Tag,
    name: _FindMethodName = None,
    attrs: _StrainableAttributes = {},
    recursive: bool = True,
    string: Optional[_StrainableString] = None,
    **kwargs: _StrainableAttribute,
) -> _OneElement:

    """
    wrapper for Tag.find() that behaves like find() but raises if element is not found.
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
        raise ValueError(f"require_find: Element not found: {name!r}, {attrs!r}, {kwargs!r}")
    return cast(Tag, result)

def require_find_parent(
        page_element: PageElement,
        name: _FindMethodName = None,
        attrs: _StrainableAttributes = {},
        **kwargs: _StrainableAttribute,
    ) -> Tag:

    result = page_element.find_parent(
        name=name,
        attrs=attrs,
        **kwargs,
    )
    if result is None or not isinstance(result, Tag):
        raise ValueError(
            f"require_find_parent: Element not found: {name!r}, {attrs!r}, {kwargs!r}")
    return cast(Tag, result)
