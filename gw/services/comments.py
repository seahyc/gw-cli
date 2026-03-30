"""
Google Workspace comments service functions for the gw CLI.

Ported from core/comments.py. Works on Docs, Sheets, and Slides via Drive API.
For Google Docs, comments can be anchored to specific text positions using
the Docs API to resolve text offsets.
"""

import json
import logging

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers for doc anchor building
# ---------------------------------------------------------------------------

def _find_text_position_in_doc(docs_service, file_id: str, quoted_text: str) -> tuple:
    """Use the Docs API to find the character offset of quoted_text in a document.

    Returns:
        (start_index, text_length, total_doc_length) or (None, None, None) if not found.
    """
    try:
        doc = (
            docs_service.documents()
            .get(documentId=file_id)
            .execute()
        )
    except Exception as e:
        logger.warning(f"[_find_text_position_in_doc] Failed to read document {file_id}: {e}")
        return None, None, None

    body = doc.get("body", {})
    content_elements = body.get("content", [])

    if not content_elements:
        return None, None, None

    total_length = content_elements[-1].get("endIndex", 0)

    offset = _find_text_offset_in_elements(content_elements, quoted_text)

    if offset is not None:
        return offset, len(quoted_text), total_length

    return None, None, None


def _find_text_offset_in_elements(elements: list, target: str) -> int | None:
    """Search for target text across document elements and return its startIndex offset."""
    for element in elements:
        if "paragraph" in element:
            para = element["paragraph"]
            para_elements = para.get("elements", [])
            if not para_elements:
                continue

            para_start = para_elements[0].get("startIndex", 0)
            para_text = ""
            for pe in para_elements:
                text_run = pe.get("textRun", {})
                if "content" in text_run:
                    para_text += text_run["content"]

            idx = para_text.find(target)
            if idx != -1:
                return para_start + idx

        elif "table" in element:
            for row in element["table"].get("tableRows", []):
                for cell in row.get("tableCells", []):
                    result = _find_text_offset_in_elements(
                        cell.get("content", []), target
                    )
                    if result is not None:
                        return result

        elif "tableOfContents" in element:
            result = _find_text_offset_in_elements(
                element["tableOfContents"].get("content", []), target
            )
            if result is not None:
                return result

    return None


def _build_docs_anchor(start_index: int, text_length: int, total_doc_length: int) -> str:
    """Build the anchor JSON string for a Google Docs comment."""
    anchor = {
        "r": "head",
        "a": [
            {
                "txt": {
                    "o": start_index,
                    "l": text_length,
                    "ml": total_doc_length,
                }
            }
        ],
    }
    return json.dumps(anchor)


# ---------------------------------------------------------------------------
# Service functions
# ---------------------------------------------------------------------------

def read_comments(service, file_id: str) -> str:
    """Read all comments from a Google Workspace file."""
    logger.info(f"[read_comments] Reading comments for {file_id}")

    response = (
        service.comments()
        .list(
            fileId=file_id,
            fields="comments(id,content,author,createdTime,modifiedTime,resolved,quotedFileContent,replies(content,author,id,createdTime,modifiedTime))",
        )
        .execute()
    )

    comments = response.get("comments", [])

    if not comments:
        return "No comments found."

    output = [f"Found {len(comments)} comments:\n"]

    for comment in comments:
        author = comment.get("author", {}).get("displayName", "Unknown")
        content = comment.get("content", "")
        created = comment.get("createdTime", "")
        resolved = comment.get("resolved", False)
        comment_id = comment.get("id", "")
        status = " [RESOLVED]" if resolved else ""

        quoted = comment.get("quotedFileContent", {}).get("value", "")
        anchor_line = f'Anchored to: "{quoted}"' if quoted else ""

        output.append(f"Comment ID: {comment_id}")
        output.append(f"Author: {author}")
        output.append(f"Created: {created}{status}")
        if anchor_line:
            output.append(anchor_line)
        output.append(f"Content: {content}")

        replies = comment.get("replies", [])
        if replies:
            output.append(f"  Replies ({len(replies)}):")
            for reply in replies:
                reply_author = reply.get("author", {}).get("displayName", "Unknown")
                reply_content = reply.get("content", "")
                reply_created = reply.get("createdTime", "")
                reply_id = reply.get("id", "")
                output.append(f"    Reply ID: {reply_id}")
                output.append(f"    Author: {reply_author}")
                output.append(f"    Created: {reply_created}")
                output.append(f"    Content: {reply_content}")

        output.append("")  # Empty line between comments

    return "\n".join(output)


def create_comment(
    service, file_id: str, content: str, quoted_text: str = "", docs_service=None
) -> str:
    """Create a new comment on a Google Workspace file.

    If quoted_text is provided AND docs_service is available (Google Docs),
    the comment will be anchored to the first occurrence of that text.
    """
    logger.info(f"[create_comment] Creating comment in {file_id}")

    body = {"content": content}
    anchor_resolved = False

    if quoted_text:
        body["quotedFileContent"] = {
            "mimeType": "text/plain",
            "value": quoted_text,
        }

        if docs_service is not None:
            start_index, text_length, total_length = _find_text_position_in_doc(
                docs_service, file_id, quoted_text
            )
            if start_index is not None:
                anchor_json = _build_docs_anchor(start_index, text_length, total_length)
                body["anchor"] = anchor_json
                anchor_resolved = True
                logger.info(
                    f"[create_comment] Anchor resolved: offset={start_index}, "
                    f"length={text_length}, doc_length={total_length}"
                )
            else:
                logger.warning(
                    f"[create_comment] Could not find quoted text "
                    f"'{quoted_text[:50]}...' in document {file_id}. "
                    f"Comment will be created without a text anchor."
                )

    comment = (
        service.comments()
        .create(
            fileId=file_id,
            body=body,
            fields="id,content,author,createdTime,modifiedTime,quotedFileContent,anchor",
        )
        .execute()
    )

    comment_id = comment.get("id", "")
    author = comment.get("author", {}).get("displayName", "Unknown")
    created = comment.get("createdTime", "")
    quoted = comment.get("quotedFileContent", {}).get("value", "")
    anchor_info = f'\nAnchored to: "{quoted}"' if quoted else ""
    if anchor_resolved:
        anchor_info += f"\nAnchor position: offset {start_index}, length {text_length}"

    return f"Comment created.\nComment ID: {comment_id}\nAuthor: {author}\nCreated: {created}{anchor_info}"


def reply_to_comment(service, file_id: str, comment_id: str, content: str) -> str:
    """Reply to a comment on a Google Workspace file."""
    logger.info(f"[reply_to_comment] Replying to comment {comment_id} in {file_id}")

    body = {"content": content}

    reply = (
        service.replies()
        .create(
            fileId=file_id,
            commentId=comment_id,
            body=body,
            fields="id,content,author,createdTime,modifiedTime",
        )
        .execute()
    )

    reply_id = reply.get("id", "")
    author = reply.get("author", {}).get("displayName", "Unknown")
    created = reply.get("createdTime", "")

    return f"Reply posted.\nReply ID: {reply_id}\nAuthor: {author}\nCreated: {created}"


def resolve_comment(service, file_id: str, comment_id: str) -> str:
    """Resolve a comment on a Google Workspace file."""
    logger.info(f"[resolve_comment] Resolving comment {comment_id} in {file_id}")

    body = {"content": "This comment has been resolved.", "action": "resolve"}

    reply = (
        service.replies()
        .create(
            fileId=file_id,
            commentId=comment_id,
            body=body,
            fields="id,content,author,createdTime,modifiedTime",
        )
        .execute()
    )

    reply_id = reply.get("id", "")
    author = reply.get("author", {}).get("displayName", "Unknown")
    created = reply.get("createdTime", "")

    return f"Comment resolved.\nResolve reply ID: {reply_id}\nAuthor: {author}\nCreated: {created}"


def edit_comment(service, file_id: str, comment_id: str, new_content: str) -> str:
    """Edit an existing comment on a Google Workspace file."""
    logger.info(f"[edit_comment] Editing comment {comment_id} in {file_id}")

    body = {"content": new_content}

    comment = (
        service.comments()
        .update(
            fileId=file_id,
            commentId=comment_id,
            body=body,
            fields="id,content,author,createdTime,modifiedTime",
        )
        .execute()
    )

    author = comment.get("author", {}).get("displayName", "Unknown")
    modified = comment.get("modifiedTime", "")

    return f"Comment updated.\nAuthor: {author}\nModified: {modified}"


def delete_comment(service, file_id: str, comment_id: str) -> str:
    """Delete a comment from a Google Workspace file."""
    logger.info(f"[delete_comment] Deleting comment {comment_id} in {file_id}")

    (
        service.comments()
        .delete(
            fileId=file_id,
            commentId=comment_id,
        )
        .execute()
    )

    return f"Comment {comment_id} deleted."


def edit_reply(
    service, file_id: str, comment_id: str, reply_id: str, new_content: str
) -> str:
    """Edit a reply on a comment in a Google Workspace file."""
    logger.info(
        f"[edit_reply] Editing reply {reply_id} on comment {comment_id} in {file_id}"
    )

    body = {"content": new_content}

    reply = (
        service.replies()
        .update(
            fileId=file_id,
            commentId=comment_id,
            replyId=reply_id,
            body=body,
            fields="id,content,author,createdTime,modifiedTime",
        )
        .execute()
    )

    author = reply.get("author", {}).get("displayName", "Unknown")
    modified = reply.get("modifiedTime", "")

    return f"Reply updated.\nAuthor: {author}\nModified: {modified}"


def delete_reply(service, file_id: str, comment_id: str, reply_id: str) -> str:
    """Delete a reply from a comment in a Google Workspace file."""
    logger.info(
        f"[delete_reply] Deleting reply {reply_id} on comment {comment_id} in {file_id}"
    )

    (
        service.replies()
        .delete(
            fileId=file_id,
            commentId=comment_id,
            replyId=reply_id,
        )
        .execute()
    )

    return f"Reply {reply_id} deleted."
