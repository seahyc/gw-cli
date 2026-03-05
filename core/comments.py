"""
Core Comments Module

This module provides reusable comment management functions for Google Workspace applications.
All Google Workspace apps (Docs, Sheets, Slides) use the Drive API for comment operations.
"""

import logging
import asyncio


from auth.service_decorator import require_google_service
from core.server import server
from core.utils import handle_http_errors

logger = logging.getLogger(__name__)


def create_comment_tools(app_name: str, file_id_param: str):
    """
    Factory function to create comment management tools for a specific Google Workspace app.

    Args:
        app_name: Name of the app (e.g., "document", "spreadsheet", "presentation")
        file_id_param: Parameter name for the file ID (e.g., "document_id", "spreadsheet_id", "presentation_id")

    Returns:
        Dict containing the four comment management functions with unique names
    """

    # Create unique function names based on the app type
    read_func_name = f"read_{app_name}_comments"
    create_func_name = f"create_{app_name}_comment"
    reply_func_name = f"reply_to_{app_name}_comment"
    resolve_func_name = f"resolve_{app_name}_comment"
    edit_comment_func_name = f"edit_{app_name}_comment"
    delete_comment_func_name = f"delete_{app_name}_comment"
    edit_reply_func_name = f"edit_{app_name}_comment_reply"
    delete_reply_func_name = f"delete_{app_name}_comment_reply"

    # Create functions without decorators first, then apply decorators with proper names
    if file_id_param == "document_id":

        @require_google_service("drive", "drive_read")
        @handle_http_errors(read_func_name, service_type="drive")
        async def read_comments(
            service, user_google_email: str, document_id: str
        ) -> str:
            """Read all comments from a Google Document."""
            return await _read_comments_impl(service, app_name, document_id)

        @require_google_service("drive", "drive_file")
        @handle_http_errors(create_func_name, service_type="drive")
        async def create_comment(
            service, user_google_email: str, document_id: str, comment_content: str
        ) -> str:
            """Create a new comment on a Google Document."""
            return await _create_comment_impl(
                service, app_name, document_id, comment_content
            )

        @require_google_service("drive", "drive_file")
        @handle_http_errors(reply_func_name, service_type="drive")
        async def reply_to_comment(
            service,
            user_google_email: str,
            document_id: str,
            comment_id: str,
            reply_content: str,
        ) -> str:
            """Reply to a specific comment in a Google Document."""
            return await _reply_to_comment_impl(
                service, app_name, document_id, comment_id, reply_content
            )

        @require_google_service("drive", "drive_file")
        @handle_http_errors(resolve_func_name, service_type="drive")
        async def resolve_comment(
            service, user_google_email: str, document_id: str, comment_id: str
        ) -> str:
            """Resolve a comment in a Google Document."""
            return await _resolve_comment_impl(
                service, app_name, document_id, comment_id
            )

        @require_google_service("drive", "drive_file")
        @handle_http_errors(edit_comment_func_name, service_type="drive")
        async def edit_comment(
            service, user_google_email: str, document_id: str, comment_id: str, new_content: str
        ) -> str:
            """Edit an existing comment on a Google Document.

            Args:
                document_id: The ID of the Google Document
                comment_id: The ID of the comment to edit
                new_content: The new content for the comment
            """
            return await _edit_comment_impl(service, app_name, document_id, comment_id, new_content)

        @require_google_service("drive", "drive_file")
        @handle_http_errors(delete_comment_func_name, service_type="drive")
        async def delete_comment(
            service, user_google_email: str, document_id: str, comment_id: str
        ) -> str:
            """Delete a comment from a Google Document.

            Args:
                document_id: The ID of the Google Document
                comment_id: The ID of the comment to delete
            """
            return await _delete_comment_impl(service, app_name, document_id, comment_id)

        @require_google_service("drive", "drive_file")
        @handle_http_errors(edit_reply_func_name, service_type="drive")
        async def edit_reply(
            service, user_google_email: str, document_id: str, comment_id: str, reply_id: str, new_content: str
        ) -> str:
            """Edit an existing reply on a comment in a Google Document.

            Args:
                document_id: The ID of the Google Document
                comment_id: The ID of the parent comment
                reply_id: The ID of the reply to edit
                new_content: The new content for the reply
            """
            return await _edit_reply_impl(service, app_name, document_id, comment_id, reply_id, new_content)

        @require_google_service("drive", "drive_file")
        @handle_http_errors(delete_reply_func_name, service_type="drive")
        async def delete_reply(
            service, user_google_email: str, document_id: str, comment_id: str, reply_id: str
        ) -> str:
            """Delete a reply from a comment in a Google Document.

            Args:
                document_id: The ID of the Google Document
                comment_id: The ID of the parent comment
                reply_id: The ID of the reply to delete
            """
            return await _delete_reply_impl(service, app_name, document_id, comment_id, reply_id)

    elif file_id_param == "spreadsheet_id":

        @require_google_service("drive", "drive_read")
        @handle_http_errors(read_func_name, service_type="drive")
        async def read_comments(
            service, user_google_email: str, spreadsheet_id: str
        ) -> str:
            """Read all comments from a Google Spreadsheet."""
            return await _read_comments_impl(service, app_name, spreadsheet_id)

        @require_google_service("drive", "drive_file")
        @handle_http_errors(create_func_name, service_type="drive")
        async def create_comment(
            service, user_google_email: str, spreadsheet_id: str, comment_content: str
        ) -> str:
            """Create a new comment on a Google Spreadsheet."""
            return await _create_comment_impl(
                service, app_name, spreadsheet_id, comment_content
            )

        @require_google_service("drive", "drive_file")
        @handle_http_errors(reply_func_name, service_type="drive")
        async def reply_to_comment(
            service,
            user_google_email: str,
            spreadsheet_id: str,
            comment_id: str,
            reply_content: str,
        ) -> str:
            """Reply to a specific comment in a Google Spreadsheet."""
            return await _reply_to_comment_impl(
                service, app_name, spreadsheet_id, comment_id, reply_content
            )

        @require_google_service("drive", "drive_file")
        @handle_http_errors(resolve_func_name, service_type="drive")
        async def resolve_comment(
            service, user_google_email: str, spreadsheet_id: str, comment_id: str
        ) -> str:
            """Resolve a comment in a Google Spreadsheet."""
            return await _resolve_comment_impl(
                service, app_name, spreadsheet_id, comment_id
            )

        @require_google_service("drive", "drive_file")
        @handle_http_errors(edit_comment_func_name, service_type="drive")
        async def edit_comment(
            service, user_google_email: str, spreadsheet_id: str, comment_id: str, new_content: str
        ) -> str:
            """Edit an existing comment on a Google Spreadsheet.

            Args:
                spreadsheet_id: The ID of the Google Spreadsheet
                comment_id: The ID of the comment to edit
                new_content: The new content for the comment
            """
            return await _edit_comment_impl(service, app_name, spreadsheet_id, comment_id, new_content)

        @require_google_service("drive", "drive_file")
        @handle_http_errors(delete_comment_func_name, service_type="drive")
        async def delete_comment(
            service, user_google_email: str, spreadsheet_id: str, comment_id: str
        ) -> str:
            """Delete a comment from a Google Spreadsheet.

            Args:
                spreadsheet_id: The ID of the Google Spreadsheet
                comment_id: The ID of the comment to delete
            """
            return await _delete_comment_impl(service, app_name, spreadsheet_id, comment_id)

        @require_google_service("drive", "drive_file")
        @handle_http_errors(edit_reply_func_name, service_type="drive")
        async def edit_reply(
            service, user_google_email: str, spreadsheet_id: str, comment_id: str, reply_id: str, new_content: str
        ) -> str:
            """Edit an existing reply on a comment in a Google Spreadsheet.

            Args:
                spreadsheet_id: The ID of the Google Spreadsheet
                comment_id: The ID of the parent comment
                reply_id: The ID of the reply to edit
                new_content: The new content for the reply
            """
            return await _edit_reply_impl(service, app_name, spreadsheet_id, comment_id, reply_id, new_content)

        @require_google_service("drive", "drive_file")
        @handle_http_errors(delete_reply_func_name, service_type="drive")
        async def delete_reply(
            service, user_google_email: str, spreadsheet_id: str, comment_id: str, reply_id: str
        ) -> str:
            """Delete a reply from a comment in a Google Spreadsheet.

            Args:
                spreadsheet_id: The ID of the Google Spreadsheet
                comment_id: The ID of the parent comment
                reply_id: The ID of the reply to delete
            """
            return await _delete_reply_impl(service, app_name, spreadsheet_id, comment_id, reply_id)

    elif file_id_param == "presentation_id":

        @require_google_service("drive", "drive_read")
        @handle_http_errors(read_func_name, service_type="drive")
        async def read_comments(
            service, user_google_email: str, presentation_id: str
        ) -> str:
            """Read all comments from a Google Presentation."""
            return await _read_comments_impl(service, app_name, presentation_id)

        @require_google_service("drive", "drive_file")
        @handle_http_errors(create_func_name, service_type="drive")
        async def create_comment(
            service, user_google_email: str, presentation_id: str, comment_content: str
        ) -> str:
            """Create a new comment on a Google Presentation."""
            return await _create_comment_impl(
                service, app_name, presentation_id, comment_content
            )

        @require_google_service("drive", "drive_file")
        @handle_http_errors(reply_func_name, service_type="drive")
        async def reply_to_comment(
            service,
            user_google_email: str,
            presentation_id: str,
            comment_id: str,
            reply_content: str,
        ) -> str:
            """Reply to a specific comment in a Google Presentation."""
            return await _reply_to_comment_impl(
                service, app_name, presentation_id, comment_id, reply_content
            )

        @require_google_service("drive", "drive_file")
        @handle_http_errors(resolve_func_name, service_type="drive")
        async def resolve_comment(
            service, user_google_email: str, presentation_id: str, comment_id: str
        ) -> str:
            """Resolve a comment in a Google Presentation."""
            return await _resolve_comment_impl(
                service, app_name, presentation_id, comment_id
            )

        @require_google_service("drive", "drive_file")
        @handle_http_errors(edit_comment_func_name, service_type="drive")
        async def edit_comment(
            service, user_google_email: str, presentation_id: str, comment_id: str, new_content: str
        ) -> str:
            """Edit an existing comment on a Google Presentation.

            Args:
                presentation_id: The ID of the Google Presentation
                comment_id: The ID of the comment to edit
                new_content: The new content for the comment
            """
            return await _edit_comment_impl(service, app_name, presentation_id, comment_id, new_content)

        @require_google_service("drive", "drive_file")
        @handle_http_errors(delete_comment_func_name, service_type="drive")
        async def delete_comment(
            service, user_google_email: str, presentation_id: str, comment_id: str
        ) -> str:
            """Delete a comment from a Google Presentation.

            Args:
                presentation_id: The ID of the Google Presentation
                comment_id: The ID of the comment to delete
            """
            return await _delete_comment_impl(service, app_name, presentation_id, comment_id)

        @require_google_service("drive", "drive_file")
        @handle_http_errors(edit_reply_func_name, service_type="drive")
        async def edit_reply(
            service, user_google_email: str, presentation_id: str, comment_id: str, reply_id: str, new_content: str
        ) -> str:
            """Edit an existing reply on a comment in a Google Presentation.

            Args:
                presentation_id: The ID of the Google Presentation
                comment_id: The ID of the parent comment
                reply_id: The ID of the reply to edit
                new_content: The new content for the reply
            """
            return await _edit_reply_impl(service, app_name, presentation_id, comment_id, reply_id, new_content)

        @require_google_service("drive", "drive_file")
        @handle_http_errors(delete_reply_func_name, service_type="drive")
        async def delete_reply(
            service, user_google_email: str, presentation_id: str, comment_id: str, reply_id: str
        ) -> str:
            """Delete a reply from a comment in a Google Presentation.

            Args:
                presentation_id: The ID of the Google Presentation
                comment_id: The ID of the parent comment
                reply_id: The ID of the reply to delete
            """
            return await _delete_reply_impl(service, app_name, presentation_id, comment_id, reply_id)

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
    logger.info(f"[read_{app_name}_comments] Reading comments for {app_name} {file_id}")

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
        return f"No comments found in {app_name} {file_id}"

    output = [f"Found {len(comments)} comments in {app_name} {file_id}:\\n"]

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
    logger.info(f"[create_{app_name}_comment] Creating comment in {app_name} {file_id}")

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

    return f"Comment created successfully!\\nComment ID: {comment_id}\\nAuthor: {author}\\nCreated: {created}\\nContent: {comment_content}"


async def _reply_to_comment_impl(
    service, app_name: str, file_id: str, comment_id: str, reply_content: str
) -> str:
    """Implementation for replying to a comment on any Google Workspace file."""
    logger.info(
        f"[reply_to_{app_name}_comment] Replying to comment {comment_id} in {app_name} {file_id}"
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

    return f"Reply posted successfully!\\nReply ID: {reply_id}\\nAuthor: {author}\\nCreated: {created}\\nContent: {reply_content}"


async def _resolve_comment_impl(
    service, app_name: str, file_id: str, comment_id: str
) -> str:
    """Implementation for resolving a comment on any Google Workspace file."""
    logger.info(
        f"[resolve_{app_name}_comment] Resolving comment {comment_id} in {app_name} {file_id}"
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

    return f"Comment {comment_id} has been resolved successfully.\\nResolve reply ID: {reply_id}\\nAuthor: {author}\\nCreated: {created}"


async def _edit_comment_impl(
    service, app_name: str, file_id: str, comment_id: str, new_content: str
) -> str:
    """Implementation for editing a comment on any Google Workspace file."""
    logger.info(
        f"[edit_{app_name}_comment] Editing comment {comment_id} in {app_name} {file_id}"
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

    return f"Comment {comment_id} updated successfully!\nAuthor: {author}\nModified: {modified}\nNew content: {new_content}"


async def _delete_comment_impl(
    service, app_name: str, file_id: str, comment_id: str
) -> str:
    """Implementation for deleting a comment on any Google Workspace file."""
    logger.info(
        f"[delete_{app_name}_comment] Deleting comment {comment_id} in {app_name} {file_id}"
    )

    await asyncio.to_thread(
        service.comments()
        .delete(
            fileId=file_id,
            commentId=comment_id,
        )
        .execute
    )

    return f"Comment {comment_id} deleted successfully from {app_name} {file_id}."


async def _edit_reply_impl(
    service, app_name: str, file_id: str, comment_id: str, reply_id: str, new_content: str
) -> str:
    """Implementation for editing a reply on any Google Workspace file."""
    logger.info(
        f"[edit_{app_name}_comment_reply] Editing reply {reply_id} on comment {comment_id} in {app_name} {file_id}"
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

    return f"Reply {reply_id} updated successfully!\nAuthor: {author}\nModified: {modified}\nNew content: {new_content}"


async def _delete_reply_impl(
    service, app_name: str, file_id: str, comment_id: str, reply_id: str
) -> str:
    """Implementation for deleting a reply on any Google Workspace file."""
    logger.info(
        f"[delete_{app_name}_comment_reply] Deleting reply {reply_id} on comment {comment_id} in {app_name} {file_id}"
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

    return f"Reply {reply_id} deleted successfully from comment {comment_id} in {app_name} {file_id}."
