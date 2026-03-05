"""
Core Comments Module

Reusable comment management functions for Google Workspace apps.
All apps (Docs, Sheets, Slides) use the Drive API for comment operations.
"""

import logging
import asyncio


from auth.service_decorator import require_google_service
from core.server import server
from core.utils import handle_http_errors

logger = logging.getLogger(__name__)


def create_comment_tools(app_name: str, file_id_param: str):
    """Factory to create comment tools for a Google Workspace app."""

    # Create unique function names based on the app type
    read_func_name = f"read_{app_name}_comments"
    create_func_name = f"create_{app_name}_comment"
    reply_func_name = f"reply_to_{app_name}_comment"
    resolve_func_name = f"resolve_{app_name}_comment"
    edit_comment_func_name = f"edit_{app_name}_comment"
    delete_comment_func_name = f"delete_{app_name}_comment"
    edit_reply_func_name = f"edit_{app_name}_comment_reply"
    delete_reply_func_name = f"delete_{app_name}_comment_reply"

    @require_google_service("drive", "drive_read")
    @handle_http_errors(read_func_name, service_type="drive")
    async def read_comments(
        service, user_google_email: str = "", file_id: str = ""
    ) -> str:
        """Read all comments from a Google Workspace file."""
        return await _read_comments_impl(service, app_name, file_id)

    @require_google_service("drive", "drive_file")
    @handle_http_errors(create_func_name, service_type="drive")
    async def create_comment(
        service, user_google_email: str = "", file_id: str = "", comment_content: str = ""
    ) -> str:
        """Create a new comment on a Google Workspace file."""
        return await _create_comment_impl(service, app_name, file_id, comment_content)

    @require_google_service("drive", "drive_file")
    @handle_http_errors(reply_func_name, service_type="drive")
    async def reply_to_comment(
        service,
        user_google_email: str = "",
        file_id: str = "",
        comment_id: str = "",
        reply_content: str = "",
    ) -> str:
        """Reply to a comment on a Google Workspace file."""
        return await _reply_to_comment_impl(
            service, app_name, file_id, comment_id, reply_content
        )

    @require_google_service("drive", "drive_file")
    @handle_http_errors(resolve_func_name, service_type="drive")
    async def resolve_comment(
        service, user_google_email: str = "", file_id: str = "", comment_id: str = ""
    ) -> str:
        """Resolve a comment on a Google Workspace file."""
        return await _resolve_comment_impl(service, app_name, file_id, comment_id)

    @require_google_service("drive", "drive_file")
    @handle_http_errors(edit_comment_func_name, service_type="drive")
    async def edit_comment(
        service, user_google_email: str = "", file_id: str = "", comment_id: str = "", new_content: str = ""
    ) -> str:
        """Edit an existing comment on a Google Workspace file."""
        return await _edit_comment_impl(service, app_name, file_id, comment_id, new_content)

    @require_google_service("drive", "drive_file")
    @handle_http_errors(delete_comment_func_name, service_type="drive")
    async def delete_comment(
        service, user_google_email: str = "", file_id: str = "", comment_id: str = ""
    ) -> str:
        """Delete a comment from a Google Workspace file."""
        return await _delete_comment_impl(service, app_name, file_id, comment_id)

    @require_google_service("drive", "drive_file")
    @handle_http_errors(edit_reply_func_name, service_type="drive")
    async def edit_reply(
        service, user_google_email: str = "", file_id: str = "", comment_id: str = "", reply_id: str = "", new_content: str = ""
    ) -> str:
        """Edit a reply on a comment in a Google Workspace file."""
        return await _edit_reply_impl(service, app_name, file_id, comment_id, reply_id, new_content)

    @require_google_service("drive", "drive_file")
    @handle_http_errors(delete_reply_func_name, service_type="drive")
    async def delete_reply(
        service, user_google_email: str = "", file_id: str = "", comment_id: str = "", reply_id: str = ""
    ) -> str:
        """Delete a reply from a comment in a Google Workspace file."""
        return await _delete_reply_impl(service, app_name, file_id, comment_id, reply_id)

    # Set the proper function names and register with server
    read_comments.__name__ = read_func_name
    create_comment.__name__ = create_func_name
    reply_to_comment.__name__ = reply_func_name
    resolve_comment.__name__ = resolve_func_name
    edit_comment.__name__ = edit_comment_func_name
    delete_comment.__name__ = delete_comment_func_name
    edit_reply.__name__ = edit_reply_func_name
    delete_reply.__name__ = delete_reply_func_name

    # Register tools with the server using the proper names
    server.tool()(read_comments)
    server.tool()(create_comment)
    server.tool()(reply_to_comment)
    server.tool()(resolve_comment)
    server.tool()(edit_comment)
    server.tool()(delete_comment)
    server.tool()(edit_reply)
    server.tool()(delete_reply)

    return {
        "read_comments": read_comments,
        "create_comment": create_comment,
        "reply_to_comment": reply_to_comment,
        "resolve_comment": resolve_comment,
        "edit_comment": edit_comment,
        "delete_comment": delete_comment,
        "edit_reply": edit_reply,
        "delete_reply": delete_reply,
    }


async def _read_comments_impl(service, app_name: str, file_id: str) -> str:
    """Implementation for reading comments from any Google Workspace file."""
    logger.info(f"[read_{app_name}_comments] Reading comments for {file_id}")

    response = await asyncio.to_thread(
        service.comments()
        .list(
            fileId=file_id,
            fields="comments(id,content,author,createdTime,modifiedTime,resolved,replies(content,author,id,createdTime,modifiedTime))",
        )
        .execute
    )

    comments = response.get("comments", [])

    if not comments:
        return "No comments found."

    output = [f"Found {len(comments)} comments:\\n"]

    for comment in comments:
        author = comment.get("author", {}).get("displayName", "Unknown")
        content = comment.get("content", "")
        created = comment.get("createdTime", "")
        resolved = comment.get("resolved", False)
        comment_id = comment.get("id", "")
        status = " [RESOLVED]" if resolved else ""

        output.append(f"Comment ID: {comment_id}")
        output.append(f"Author: {author}")
        output.append(f"Created: {created}{status}")
        output.append(f"Content: {content}")

        # Add replies if any
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

    return "\\n".join(output)


async def _create_comment_impl(
    service, app_name: str, file_id: str, comment_content: str
) -> str:
    """Implementation for creating a comment on any Google Workspace file."""
    logger.info(f"[create_{app_name}_comment] Creating comment in {file_id}")

    body = {"content": comment_content}

    comment = await asyncio.to_thread(
        service.comments()
        .create(
            fileId=file_id,
            body=body,
            fields="id,content,author,createdTime,modifiedTime",
        )
        .execute
    )

    comment_id = comment.get("id", "")
    author = comment.get("author", {}).get("displayName", "Unknown")
    created = comment.get("createdTime", "")

    return f"Comment created.\\nComment ID: {comment_id}\\nAuthor: {author}\\nCreated: {created}"


async def _reply_to_comment_impl(
    service, app_name: str, file_id: str, comment_id: str, reply_content: str
) -> str:
    """Implementation for replying to a comment on any Google Workspace file."""
    logger.info(
        f"[reply_to_{app_name}_comment] Replying to comment {comment_id} in {file_id}"
    )

    body = {"content": reply_content}

    reply = await asyncio.to_thread(
        service.replies()
        .create(
            fileId=file_id,
            commentId=comment_id,
            body=body,
            fields="id,content,author,createdTime,modifiedTime",
        )
        .execute
    )

    reply_id = reply.get("id", "")
    author = reply.get("author", {}).get("displayName", "Unknown")
    created = reply.get("createdTime", "")

    return f"Reply posted.\\nReply ID: {reply_id}\\nAuthor: {author}\\nCreated: {created}"


async def _resolve_comment_impl(
    service, app_name: str, file_id: str, comment_id: str
) -> str:
    """Implementation for resolving a comment on any Google Workspace file."""
    logger.info(
        f"[resolve_{app_name}_comment] Resolving comment {comment_id} in {file_id}"
    )

    body = {"content": "This comment has been resolved.", "action": "resolve"}

    reply = await asyncio.to_thread(
        service.replies()
        .create(
            fileId=file_id,
            commentId=comment_id,
            body=body,
            fields="id,content,author,createdTime,modifiedTime",
        )
        .execute
    )

    reply_id = reply.get("id", "")
    author = reply.get("author", {}).get("displayName", "Unknown")
    created = reply.get("createdTime", "")

    return f"Comment resolved.\\nResolve reply ID: {reply_id}\\nAuthor: {author}\\nCreated: {created}"


async def _edit_comment_impl(
    service, app_name: str, file_id: str, comment_id: str, new_content: str
) -> str:
    """Implementation for editing a comment on any Google Workspace file."""
    logger.info(
        f"[edit_{app_name}_comment] Editing comment {comment_id} in {file_id}"
    )

    body = {"content": new_content}

    comment = await asyncio.to_thread(
        service.comments()
        .update(
            fileId=file_id,
            commentId=comment_id,
            body=body,
            fields="id,content,author,createdTime,modifiedTime",
        )
        .execute
    )

    author = comment.get("author", {}).get("displayName", "Unknown")
    modified = comment.get("modifiedTime", "")

    return f"Comment updated.\\nAuthor: {author}\\nModified: {modified}"


async def _delete_comment_impl(
    service, app_name: str, file_id: str, comment_id: str
) -> str:
    """Implementation for deleting a comment on any Google Workspace file."""
    logger.info(
        f"[delete_{app_name}_comment] Deleting comment {comment_id} in {file_id}"
    )

    await asyncio.to_thread(
        service.comments()
        .delete(
            fileId=file_id,
            commentId=comment_id,
        )
        .execute
    )

    return f"Comment {comment_id} deleted."


async def _edit_reply_impl(
    service, app_name: str, file_id: str, comment_id: str, reply_id: str, new_content: str
) -> str:
    """Implementation for editing a reply on any Google Workspace file."""
    logger.info(
        f"[edit_{app_name}_comment_reply] Editing reply {reply_id} on comment {comment_id} in {file_id}"
    )

    body = {"content": new_content}

    reply = await asyncio.to_thread(
        service.replies()
        .update(
            fileId=file_id,
            commentId=comment_id,
            replyId=reply_id,
            body=body,
            fields="id,content,author,createdTime,modifiedTime",
        )
        .execute
    )

    author = reply.get("author", {}).get("displayName", "Unknown")
    modified = reply.get("modifiedTime", "")

    return f"Reply updated.\\nAuthor: {author}\\nModified: {modified}"


async def _delete_reply_impl(
    service, app_name: str, file_id: str, comment_id: str, reply_id: str
) -> str:
    """Implementation for deleting a reply on any Google Workspace file."""
    logger.info(
        f"[delete_{app_name}_comment_reply] Deleting reply {reply_id} on comment {comment_id} in {file_id}"
    )

    await asyncio.to_thread(
        service.replies()
        .delete(
            fileId=file_id,
            commentId=comment_id,
            replyId=reply_id,
        )
        .execute
    )

    return f"Reply {reply_id} deleted."
