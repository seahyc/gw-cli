"""
Google Slides service functions for the gw CLI.

Ported from gslides/slides_tools.py with MCP decorators removed.
"""

import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


def create_presentation(service, title: str = "Untitled Presentation") -> str:
    """Create a new Google Slides presentation."""
    logger.info(f"[create_presentation] Invoked. Title: '{title}'")

    body = {"title": title}

    result = service.presentations().create(body=body).execute()

    file_id = result.get("presentationId")
    presentation_url = f"https://docs.google.com/presentation/d/{file_id}/edit"

    confirmation_message = f"""Presentation created:
- Title: {title}
- ID: {file_id}
- URL: {presentation_url}
- Slides: {len(result.get("slides", []))} slide(s)"""

    logger.info(f"Presentation created successfully: {file_id}")
    return confirmation_message


def get_presentation(service, file_id: str) -> str:
    """Get details about a Google Slides presentation."""
    logger.info(f"[get_presentation] Invoked. ID: '{file_id}'")

    result = (
        service.presentations()
        .get(presentationId=file_id)
        .execute()
    )

    title = result.get("title", "Untitled")
    slides = result.get("slides", [])
    page_size = result.get("pageSize", {})

    slides_info = []
    for i, slide in enumerate(slides, 1):
        slide_id = slide.get("objectId", "Unknown")
        page_elements = slide.get("pageElements", [])

        slide_text = ""
        try:
            texts_from_elements = []
            for page_element in slide.get("pageElements", []):
                shape = page_element.get("shape", None)
                if shape and shape.get("text", None):
                    text = shape.get("text", None)
                    if text:
                        text_elements_in_shape = []
                        for text_element in text.get("textElements", []):
                            text_run = text_element.get("textRun", None)
                            if text_run:
                                content = text_run.get("content", None)
                                if content:
                                    start_index = text_element.get("startIndex", 0)
                                    text_elements_in_shape.append(
                                        (start_index, content)
                                    )

                        if text_elements_in_shape:
                            text_elements_in_shape.sort(key=lambda item: item[0])
                            full_text_from_shape = "".join(
                                [item[1] for item in text_elements_in_shape]
                            )
                            texts_from_elements.append(full_text_from_shape)

            slide_text = "\n".join(texts_from_elements)
            slide_text_rows = slide_text.split("\n")
            slide_text_rows = [row for row in slide_text_rows if len(row.strip()) > 0]
            if slide_text_rows:
                slide_text_rows = ["    > " + row for row in slide_text_rows]
                slide_text = "\n" + "\n".join(slide_text_rows)
            else:
                slide_text = ""
        except Exception as e:
            logger.warning(f"Failed to extract text from the slide {slide_id}: {e}")
            slide_text = f"<failed to extract text: {type(e)}, {e}>"

        slides_info.append(
            f"  Slide {i}: ID {slide_id}, {len(page_elements)} element(s), text: {slide_text if slide_text else 'empty'}"
        )

    confirmation_message = f"""Presentation details:
- Title: {title}
- URL: https://docs.google.com/presentation/d/{file_id}/edit
- Total Slides: {len(slides)}
- Page Size: {page_size.get("width", {}).get("magnitude", "Unknown")} x {page_size.get("height", {}).get("magnitude", "Unknown")} {page_size.get("width", {}).get("unit", "")}

Slides Breakdown:
{chr(10).join(slides_info) if slides_info else "  No slides found"}"""

    logger.info(f"Presentation retrieved successfully: {file_id}")
    return confirmation_message


def batch_update_presentation(
    service,
    file_id: str,
    requests: List[Dict[str, Any]] = [],
) -> str:
    """Apply batch updates to a Google Slides presentation."""
    logger.info(
        f"[batch_update_presentation] Invoked. ID: '{file_id}', Requests: {len(requests)}"
    )

    body = {"requests": requests}

    result = (
        service.presentations()
        .batchUpdate(presentationId=file_id, body=body)
        .execute()
    )

    replies = result.get("replies", [])

    confirmation_message = f"""Batch update completed:
- Requests Applied: {len(requests)}
- Replies Received: {len(replies)}"""

    if replies:
        confirmation_message += "\n\nResults:"
        for i, reply in enumerate(replies, 1):
            if "createSlide" in reply:
                slide_id = reply["createSlide"].get("objectId", "Unknown")
                confirmation_message += f"\n  {i}: Created slide {slide_id}"
            elif "createShape" in reply:
                shape_id = reply["createShape"].get("objectId", "Unknown")
                confirmation_message += f"\n  {i}: Created shape {shape_id}"
            else:
                confirmation_message += f"\n  {i}: Completed"

    logger.info(f"Batch update completed for {file_id}")
    return confirmation_message


def get_page(service, file_id: str, page_object_id: str) -> str:
    """Get details about a specific slide in a presentation."""
    logger.info(
        f"[get_page] Invoked. Presentation: '{file_id}', Page: '{page_object_id}'"
    )

    result = (
        service.presentations()
        .pages()
        .get(presentationId=file_id, pageObjectId=page_object_id)
        .execute()
    )

    page_type = result.get("pageType", "Unknown")
    page_elements = result.get("pageElements", [])

    elements_info = []
    for element in page_elements:
        element_id = element.get("objectId", "Unknown")
        if "shape" in element:
            shape_type = element["shape"].get("shapeType", "Unknown")
            elements_info.append(f"  Shape: ID {element_id}, Type: {shape_type}")
        elif "table" in element:
            table = element["table"]
            rows = table.get("rows", 0)
            cols = table.get("columns", 0)
            elements_info.append(f"  Table: ID {element_id}, Size: {rows}x{cols}")
        elif "line" in element:
            line_type = element["line"].get("lineType", "Unknown")
            elements_info.append(f"  Line: ID {element_id}, Type: {line_type}")
        else:
            elements_info.append(f"  Element: ID {element_id}, Type: Unknown")

    confirmation_message = f"""Page details:
- Page ID: {page_object_id}
- Page Type: {page_type}
- Total Elements: {len(page_elements)}

Page Elements:
{chr(10).join(elements_info) if elements_info else "  No elements found"}"""

    logger.info(f"Page retrieved successfully: {page_object_id}")
    return confirmation_message


def get_page_thumbnail(
    service,
    file_id: str,
    page_object_id: str,
    thumbnail_size: str = "MEDIUM",
) -> str:
    """Generate a thumbnail URL for a slide."""
    logger.info(
        f"[get_page_thumbnail] Invoked. Presentation: '{file_id}', "
        f"Page: '{page_object_id}', Size: '{thumbnail_size}'"
    )

    result = (
        service.presentations()
        .pages()
        .getThumbnail(
            presentationId=file_id,
            pageObjectId=page_object_id,
            thumbnailProperties_thumbnailSize=thumbnail_size,
            thumbnailProperties_mimeType="PNG",
        )
        .execute()
    )

    thumbnail_url = result.get("contentUrl", "")

    confirmation_message = f"""Thumbnail generated:
- Thumbnail Size: {thumbnail_size}
- URL: {thumbnail_url}"""

    logger.info(f"Thumbnail generated for page {page_object_id}")
    return confirmation_message
