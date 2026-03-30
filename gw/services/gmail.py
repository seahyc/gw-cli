"""
Gmail service layer for the gw CLI.

Provides synchronous functions for interacting with the Gmail API.
"""

import base64
import logging
import ssl
import time
from html.parser import HTMLParser
from typing import Optional, List, Dict, Literal, Any

from email.mime.text import MIMEText

logger = logging.getLogger(__name__)

GMAIL_BATCH_SIZE = 25
GMAIL_REQUEST_DELAY = 0.1
HTML_BODY_TRUNCATE_LIMIT = 20000
GMAIL_METADATA_HEADERS = ["Subject", "From", "To", "Cc", "Message-ID", "Date"]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


class _HTMLTextExtractor(HTMLParser):
    """Extract readable text from HTML using stdlib."""

    def __init__(self):
        super().__init__()
        self._text = []
        self._skip = False

    def handle_starttag(self, tag, attrs):
        self._skip = tag in ("script", "style")

    def handle_endtag(self, tag):
        if tag in ("script", "style"):
            self._skip = False

    def handle_data(self, data):
        if not self._skip:
            self._text.append(data)

    def get_text(self) -> str:
        return " ".join("".join(self._text).split())


def _html_to_text(html: str) -> str:
    """Convert HTML to readable plain text."""
    try:
        parser = _HTMLTextExtractor()
        parser.feed(html)
        return parser.get_text()
    except Exception:
        return html


def _extract_message_body(payload):
    """
    Helper function to extract plain text body from a Gmail message payload.

    Args:
        payload (dict): The message payload from Gmail API

    Returns:
        str: The plain text body content, or empty string if not found
    """
    bodies = _extract_message_bodies(payload)
    return bodies.get("text", "")


def _extract_message_bodies(payload):
    """
    Helper function to extract both plain text and HTML bodies from a Gmail message payload.

    Args:
        payload (dict): The message payload from Gmail API

    Returns:
        dict: Dictionary with 'text' and 'html' keys containing body content
    """
    text_body = ""
    html_body = ""
    parts = [payload] if "parts" not in payload else payload.get("parts", [])

    part_queue = list(parts)  # Use a queue for BFS traversal of parts
    while part_queue:
        part = part_queue.pop(0)
        mime_type = part.get("mimeType", "")
        body_data = part.get("body", {}).get("data")

        if body_data:
            try:
                decoded_data = base64.urlsafe_b64decode(body_data).decode(
                    "utf-8", errors="ignore"
                )
                if mime_type == "text/plain" and not text_body:
                    text_body = decoded_data
                elif mime_type == "text/html" and not html_body:
                    html_body = decoded_data
            except Exception as e:
                logger.warning(f"Failed to decode body part: {e}")

        # Add sub-parts to queue for multipart messages
        if mime_type.startswith("multipart/") and "parts" in part:
            part_queue.extend(part.get("parts", []))

    # Check the main payload if it has body data directly
    if payload.get("body", {}).get("data"):
        try:
            decoded_data = base64.urlsafe_b64decode(payload["body"]["data"]).decode(
                "utf-8", errors="ignore"
            )
            mime_type = payload.get("mimeType", "")
            if mime_type == "text/plain" and not text_body:
                text_body = decoded_data
            elif mime_type == "text/html" and not html_body:
                html_body = decoded_data
        except Exception as e:
            logger.warning(f"Failed to decode main payload body: {e}")

    return {"text": text_body, "html": html_body}


def _format_body_content(text_body: str, html_body: str) -> str:
    """
    Helper function to format message body content with HTML fallback and truncation.
    """
    text_stripped = text_body.strip()
    html_stripped = html_body.strip()

    use_html = html_stripped and (
        not text_stripped
        or "<!--" in text_stripped
        or len(html_stripped) > len(text_stripped) * 50
    )

    if use_html:
        content = _html_to_text(html_stripped)
        if len(content) > HTML_BODY_TRUNCATE_LIMIT:
            content = content[:HTML_BODY_TRUNCATE_LIMIT] + "\n\n[Content truncated...]"
        return content
    elif text_stripped:
        return text_body
    else:
        return "[No readable content found]"


def _extract_attachments(payload: dict) -> List[Dict[str, Any]]:
    """Extract attachment metadata from a Gmail message payload."""
    attachments = []

    def search_parts(part):
        if part.get("filename") and part.get("body", {}).get("attachmentId"):
            attachments.append(
                {
                    "filename": part["filename"],
                    "mimeType": part.get("mimeType", "application/octet-stream"),
                    "size": part.get("body", {}).get("size", 0),
                    "attachmentId": part["body"]["attachmentId"],
                }
            )
        if "parts" in part:
            for subpart in part["parts"]:
                search_parts(subpart)

    search_parts(payload)
    return attachments


def _extract_headers(payload: dict, header_names: List[str]) -> Dict[str, str]:
    """Extract specified headers from a Gmail message payload."""
    headers = {}
    target_headers = {name.lower(): name for name in header_names}
    for header in payload.get("headers", []):
        header_name_lower = header["name"].lower()
        if header_name_lower in target_headers:
            headers[target_headers[header_name_lower]] = header["value"]
    return headers


def _prepare_gmail_message(
    subject: str,
    body: str,
    to: Optional[str] = None,
    cc: Optional[str] = None,
    bcc: Optional[str] = None,
    thread_id: Optional[str] = None,
    in_reply_to: Optional[str] = None,
    references: Optional[str] = None,
    body_format: Literal["plain", "html"] = "plain",
    from_email: Optional[str] = None,
    attachment_paths: Optional[list[str]] = None,
) -> tuple[str, Optional[str]]:
    """
    Prepare a Gmail message with threading and attachment support.

    Returns:
        Tuple of (raw_message, thread_id) where raw_message is base64 encoded
    """
    reply_subject = subject
    if in_reply_to and not subject.lower().startswith("re:"):
        reply_subject = f"Re: {subject}"

    normalized_format = body_format.lower()
    if normalized_format not in {"plain", "html"}:
        raise ValueError("body_format must be either 'plain' or 'html'.")

    if attachment_paths:
        from email.mime.multipart import MIMEMultipart
        from email.mime.base import MIMEBase
        from email import encoders
        import mimetypes
        from pathlib import Path

        message = MIMEMultipart()
        message.attach(MIMEText(body, normalized_format))

        for file_path in attachment_paths:
            path = Path(file_path)
            if not path.exists():
                raise FileNotFoundError(f"Attachment not found: {file_path}")
            mime_type, _ = mimetypes.guess_type(str(path))
            if mime_type is None:
                mime_type = "application/octet-stream"
            main_type, sub_type = mime_type.split("/", 1)
            with open(path, "rb") as f:
                part = MIMEBase(main_type, sub_type)
                part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", "attachment", filename=path.name)
            message.attach(part)
    else:
        message = MIMEText(body, normalized_format)

    message["Subject"] = reply_subject

    if from_email:
        message["From"] = from_email
    if to:
        message["To"] = to
    if cc:
        message["Cc"] = cc
    if bcc:
        message["Bcc"] = bcc
    if in_reply_to:
        message["In-Reply-To"] = in_reply_to
    if references:
        message["References"] = references

    raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
    return raw_message, thread_id


def _generate_gmail_web_url(item_id: str, account_index: int = 0) -> str:
    """Generate Gmail web interface URL for a message or thread ID."""
    return f"https://mail.google.com/mail/u/{account_index}/#all/{item_id}"


def _format_gmail_results_plain(
    messages: list, query: str, next_page_token: Optional[str] = None
) -> str:
    """Format Gmail search results in clean, LLM-friendly plain text."""
    if not messages:
        return f"No messages found for query: '{query}'"

    lines = [
        f"Found {len(messages)} messages matching '{query}':",
        "",
        "MESSAGES:",
    ]

    for i, msg in enumerate(messages, 1):
        if not msg or not isinstance(msg, dict):
            lines.extend(
                [
                    f"  {i}. Message: Invalid message data",
                    "     Error: Message object is null or malformed",
                    "",
                ]
            )
            continue

        message_id = msg.get("id")
        thread_id = msg.get("threadId")

        if not message_id:
            message_id = "unknown"
        if not thread_id:
            thread_id = "unknown"

        message_url = _generate_gmail_web_url(message_id) if message_id != "unknown" else "N/A"
        thread_url = _generate_gmail_web_url(thread_id) if thread_id != "unknown" else "N/A"

        lines.extend(
            [
                f"  {i}. Message ID: {message_id}",
                f"     Web Link: {message_url}",
                f"     Thread ID: {thread_id}",
                f"     Thread Link: {thread_url}",
                "",
            ]
        )

    if next_page_token:
        lines.append("")
        lines.append(
            f"PAGINATION: To get the next page, use --page-token '{next_page_token}'"
        )

    return "\n".join(lines)


def _format_thread_content(thread_data: dict, thread_id: str) -> str:
    """Helper function to format thread content from Gmail API response."""
    messages = thread_data.get("messages", [])
    if not messages:
        return f"No messages found in thread '{thread_id}'."

    first_message = messages[0]
    first_headers = {
        h["name"]: h["value"]
        for h in first_message.get("payload", {}).get("headers", [])
    }
    thread_subject = first_headers.get("Subject", "(no subject)")

    content_lines = [
        f"Thread ID: {thread_id}",
        f"Subject: {thread_subject}",
        f"Messages: {len(messages)}",
        "",
    ]

    for i, message in enumerate(messages, 1):
        headers = {
            h["name"]: h["value"] for h in message.get("payload", {}).get("headers", [])
        }
        sender = headers.get("From", "(unknown sender)")
        date = headers.get("Date", "(unknown date)")
        subject = headers.get("Subject", "(no subject)")

        payload = message.get("payload", {})
        bodies = _extract_message_bodies(payload)
        text_body = bodies.get("text", "")
        html_body = bodies.get("html", "")
        body_data = _format_body_content(text_body, html_body)

        content_lines.extend(
            [
                f"=== Message {i} ===",
                f"From: {sender}",
                f"Date: {date}",
            ]
        )
        if subject != thread_subject:
            content_lines.append(f"Subject: {subject}")
        content_lines.extend(["", body_data, ""])

    return "\n".join(content_lines)


# ---------------------------------------------------------------------------
# Public service functions
# ---------------------------------------------------------------------------


def search_messages(
    service,
    query: str,
    max_results: int = 10,
    page_token: Optional[str] = None,
) -> str:
    """
    Search messages in a user's Gmail account based on a query.

    Args:
        service: Authenticated Gmail API service.
        query: Gmail search query string.
        max_results: Maximum number of messages to return.
        page_token: Token for retrieving the next page of results.

    Returns:
        Formatted search results string.
    """
    logger.info(f"[search_messages] Query: '{query}', Max results: {max_results}")

    request_params = {"userId": "me", "q": query, "maxResults": max_results}
    if page_token:
        request_params["pageToken"] = page_token

    response = (
        service.users()
        .messages()
        .list(**request_params)
        .execute()
    )

    if response is None:
        return f"No response received from Gmail API for query: '{query}'"

    messages = response.get("messages", [])
    if messages is None:
        messages = []

    next_page_token = response.get("nextPageToken")
    formatted_output = _format_gmail_results_plain(messages, query, next_page_token)

    logger.info(f"[search_messages] Found {len(messages)} messages")
    return formatted_output


def get_message_content(
    service,
    message_id: str,
    format: Literal["full", "metadata", "minimal"] = "full",
) -> str:
    """
    Retrieve the full content of a specific Gmail message.

    Args:
        service: Authenticated Gmail API service.
        message_id: The unique ID of the Gmail message.
        format: Message format (full, metadata, minimal).

    Returns:
        Formatted message content string.
    """
    logger.info(f"[get_message_content] Message ID: '{message_id}'")

    # Fetch message metadata first to get headers
    message_metadata = (
        service.users()
        .messages()
        .get(
            userId="me",
            id=message_id,
            format="metadata",
            metadataHeaders=GMAIL_METADATA_HEADERS,
        )
        .execute()
    )

    headers = _extract_headers(
        message_metadata.get("payload", {}), GMAIL_METADATA_HEADERS
    )
    subject = headers.get("Subject", "(no subject)")
    sender = headers.get("From", "(unknown sender)")
    to = headers.get("To", "")
    cc = headers.get("Cc", "")
    rfc822_msg_id = headers.get("Message-ID", "")

    # Now fetch the full message to get the body parts
    message_full = (
        service.users()
        .messages()
        .get(userId="me", id=message_id, format="full")
        .execute()
    )

    payload = message_full.get("payload", {})
    bodies = _extract_message_bodies(payload)
    text_body = bodies.get("text", "")
    html_body = bodies.get("html", "")
    body_data = _format_body_content(text_body, html_body)

    attachments = _extract_attachments(payload)

    content_lines = [
        f"Subject: {subject}",
        f"From:    {sender}",
        f"Date:    {headers.get('Date', '(unknown date)')}",
    ]

    if rfc822_msg_id:
        content_lines.append(f"Message-ID: {rfc822_msg_id}")
    if to:
        content_lines.append(f"To:      {to}")
    if cc:
        content_lines.append(f"Cc:      {cc}")

    content_lines.append(f"\n--- BODY ---\n{body_data or '[No text/plain body found]'}")

    if attachments:
        content_lines.append("\n--- ATTACHMENTS ---")
        for i, att in enumerate(attachments, 1):
            size_kb = att["size"] / 1024
            content_lines.append(
                f"{i}. {att['filename']} ({att['mimeType']}, {size_kb:.1f} KB)\n"
                f"   Attachment ID: {att['attachmentId']}"
            )

    return "\n".join(content_lines)


def get_messages_content_batch(
    service,
    message_ids: List[str],
    format: Literal["full", "metadata"] = "full",
) -> str:
    """
    Retrieve the content of multiple Gmail messages in a single batch request.

    Args:
        service: Authenticated Gmail API service.
        message_ids: List of Gmail message IDs to retrieve (max 25 per batch).
        format: Message format. "full" includes body, "metadata" only headers.

    Returns:
        Formatted list of message contents.
    """
    logger.info(f"[get_messages_content_batch] Message count: {len(message_ids)}")

    if not message_ids:
        raise Exception("No message IDs provided")

    output_messages = []

    for chunk_start in range(0, len(message_ids), GMAIL_BATCH_SIZE):
        chunk_ids = message_ids[chunk_start : chunk_start + GMAIL_BATCH_SIZE]
        results: Dict[str, Dict] = {}

        def _batch_callback(request_id, response, exception):
            results[request_id] = {"data": response, "error": exception}

        try:
            batch = service.new_batch_http_request(callback=_batch_callback)

            for mid in chunk_ids:
                if format == "metadata":
                    req = (
                        service.users()
                        .messages()
                        .get(
                            userId="me",
                            id=mid,
                            format="metadata",
                            metadataHeaders=GMAIL_METADATA_HEADERS,
                        )
                    )
                else:
                    req = (
                        service.users()
                        .messages()
                        .get(userId="me", id=mid, format="full")
                    )
                batch.add(req, request_id=mid)

            batch.execute()

        except Exception as batch_error:
            logger.warning(
                f"[get_messages_content_batch] Batch API failed, falling back to sequential: {batch_error}"
            )

            def fetch_message_with_retry(mid: str, max_retries: int = 3):
                for attempt in range(max_retries):
                    try:
                        if format == "metadata":
                            msg = (
                                service.users()
                                .messages()
                                .get(
                                    userId="me",
                                    id=mid,
                                    format="metadata",
                                    metadataHeaders=GMAIL_METADATA_HEADERS,
                                )
                                .execute()
                            )
                        else:
                            msg = (
                                service.users()
                                .messages()
                                .get(userId="me", id=mid, format="full")
                                .execute()
                            )
                        return mid, msg, None
                    except ssl.SSLError as ssl_error:
                        if attempt < max_retries - 1:
                            delay = 2 ** attempt
                            logger.warning(
                                f"[get_messages_content_batch] SSL error for {mid}, attempt {attempt + 1}: {ssl_error}. Retrying in {delay}s..."
                            )
                            time.sleep(delay)
                        else:
                            return mid, None, ssl_error
                    except Exception as e:
                        return mid, None, e

            for mid in chunk_ids:
                mid_result, msg_data, err = fetch_message_with_retry(mid)
                results[mid_result] = {"data": msg_data, "error": err}
                time.sleep(GMAIL_REQUEST_DELAY)

        for mid in chunk_ids:
            entry = results.get(mid, {"data": None, "error": "No result"})

            if entry["error"]:
                output_messages.append(f"Message {mid}: {entry['error']}\n")
            else:
                message = entry["data"]
                if not message:
                    output_messages.append(f"Message {mid}: No data returned\n")
                    continue

                payload = message.get("payload", {})

                if format == "metadata":
                    headers = _extract_headers(payload, GMAIL_METADATA_HEADERS)
                    subject = headers.get("Subject", "(no subject)")
                    sender = headers.get("From", "(unknown sender)")
                    to = headers.get("To", "")
                    cc = headers.get("Cc", "")
                    rfc822_msg_id = headers.get("Message-ID", "")

                    msg_output = (
                        f"Message ID: {mid}\nSubject: {subject}\nFrom: {sender}\n"
                        f"Date: {headers.get('Date', '(unknown date)')}\n"
                    )
                    if rfc822_msg_id:
                        msg_output += f"Message-ID: {rfc822_msg_id}\n"
                    if to:
                        msg_output += f"To: {to}\n"
                    if cc:
                        msg_output += f"Cc: {cc}\n"
                    msg_output += f"Web Link: {_generate_gmail_web_url(mid)}\n"
                    output_messages.append(msg_output)
                else:
                    headers = _extract_headers(payload, GMAIL_METADATA_HEADERS)
                    subject = headers.get("Subject", "(no subject)")
                    sender = headers.get("From", "(unknown sender)")
                    to = headers.get("To", "")
                    cc = headers.get("Cc", "")
                    rfc822_msg_id = headers.get("Message-ID", "")

                    bodies = _extract_message_bodies(payload)
                    text_body = bodies.get("text", "")
                    html_body = bodies.get("html", "")
                    body_data = _format_body_content(text_body, html_body)

                    msg_output = (
                        f"Message ID: {mid}\nSubject: {subject}\nFrom: {sender}\n"
                        f"Date: {headers.get('Date', '(unknown date)')}\n"
                    )
                    if rfc822_msg_id:
                        msg_output += f"Message-ID: {rfc822_msg_id}\n"
                    if to:
                        msg_output += f"To: {to}\n"
                    if cc:
                        msg_output += f"Cc: {cc}\n"
                    msg_output += (
                        f"Web Link: {_generate_gmail_web_url(mid)}\n\n{body_data}\n"
                    )
                    output_messages.append(msg_output)

    final_output = f"Retrieved {len(message_ids)} messages:\n\n"
    final_output += "\n---\n\n".join(output_messages)
    return final_output


def get_attachment_content(
    service,
    message_id: str,
    attachment_id: str,
    save_to: Optional[str] = None,
) -> str:
    """
    Download the content of a specific email attachment.

    Args:
        service: Authenticated Gmail API service.
        message_id: The ID of the Gmail message containing the attachment.
        attachment_id: The ID of the attachment to download.
        save_to: Optional file path to save the attachment to.

    Returns:
        Attachment metadata and content info.
    """
    logger.info(f"[get_attachment_content] Message ID: '{message_id}'")

    try:
        attachment = (
            service.users()
            .messages()
            .attachments()
            .get(userId="me", messageId=message_id, id=attachment_id)
            .execute()
        )
    except Exception as e:
        logger.error(f"[get_attachment_content] Failed to download attachment: {e}")
        return (
            f"Error: Failed to download attachment. The attachment ID may have changed.\n"
            f"Please fetch the message content again to get an updated attachment ID.\n\n"
            f"Error details: {str(e)}"
        )

    size_bytes = attachment.get("size", 0)
    size_kb = size_bytes / 1024 if size_bytes else 0
    base64_data = attachment.get("data", "")

    if save_to and base64_data:
        from pathlib import Path

        raw_bytes = base64.urlsafe_b64decode(base64_data)
        path = Path(save_to)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw_bytes)

        return (
            f"Attachment saved successfully!\n"
            f"Message ID: {message_id}\n"
            f"Size: {size_kb:.1f} KB ({size_bytes} bytes)\n"
            f"Saved to: {save_to}"
        )

    result_lines = [
        "Attachment downloaded successfully!",
        f"Message ID: {message_id}",
        f"Size: {size_kb:.1f} KB ({size_bytes} bytes)",
        "",
        "Base64-encoded content (first 100 characters shown):",
        f"{base64_data[:100]}...",
    ]
    return "\n".join(result_lines)


def draft_message(
    service,
    subject: str,
    body: str,
    to: Optional[str] = None,
    cc: Optional[str] = None,
    bcc: Optional[str] = None,
    body_format: Literal["plain", "html"] = "plain",
    thread_id: Optional[str] = None,
    in_reply_to: Optional[str] = None,
    references: Optional[str] = None,
    attachment_paths: Optional[list[str]] = None,
) -> str:
    """
    Create a draft email in the user's Gmail account.

    Args:
        service: Authenticated Gmail API service.
        subject: Email subject.
        body: Email body content.
        to: Recipient email address.
        cc: CC email address.
        bcc: BCC email address.
        body_format: 'plain' or 'html'.
        thread_id: Gmail thread ID to reply within.
        in_reply_to: Message-ID of the message being replied to.
        references: Chain of Message-IDs for proper threading.
        attachment_paths: List of file paths to attach.

    Returns:
        Confirmation message with draft ID.
    """
    logger.info(f"[draft_message] Subject: '{subject}'")

    raw_message, thread_id_final = _prepare_gmail_message(
        subject=subject,
        body=body,
        body_format=body_format,
        to=to,
        cc=cc,
        bcc=bcc,
        thread_id=thread_id,
        in_reply_to=in_reply_to,
        references=references,
        attachment_paths=attachment_paths,
    )

    draft_body = {"message": {"raw": raw_message}}
    if thread_id_final:
        draft_body["message"]["threadId"] = thread_id_final

    created_draft = (
        service.users()
        .drafts()
        .create(userId="me", body=draft_body)
        .execute()
    )
    draft_id = created_draft.get("id")
    return f"Draft created! Draft ID: {draft_id}"


def send_message(
    service,
    to: str,
    subject: str,
    body: str,
    cc: Optional[str] = None,
    bcc: Optional[str] = None,
    body_format: Literal["plain", "html"] = "plain",
    attachment_paths: Optional[list[str]] = None,
) -> str:
    """
    Build and send an email message.

    Args:
        service: Authenticated Gmail API service.
        to: Recipient email address.
        subject: Email subject.
        body: Email body content.
        cc: CC email address.
        bcc: BCC email address.
        body_format: 'plain' or 'html'.
        attachment_paths: List of file paths to attach.

    Returns:
        Confirmation message with sent message ID.
    """
    logger.info(f"[send_message] To: '{to}', Subject: '{subject}'")

    raw_message, _ = _prepare_gmail_message(
        subject=subject,
        body=body,
        body_format=body_format,
        to=to,
        cc=cc,
        bcc=bcc,
        attachment_paths=attachment_paths,
    )

    message_body = {"raw": raw_message}

    sent_message = (
        service.users()
        .messages()
        .send(userId="me", body=message_body)
        .execute()
    )
    message_id = sent_message.get("id")
    return f"Message sent! Message ID: {message_id}"


def get_thread_content(service, thread_id: str) -> str:
    """
    Retrieve the complete content of a Gmail conversation thread.

    Args:
        service: Authenticated Gmail API service.
        thread_id: The unique ID of the Gmail thread.

    Returns:
        Formatted thread content with all messages.
    """
    logger.info(f"[get_thread_content] Thread ID: '{thread_id}'")

    thread_response = (
        service.users()
        .threads()
        .get(userId="me", id=thread_id, format="full")
        .execute()
    )

    return _format_thread_content(thread_response, thread_id)


def get_threads_content_batch(
    service,
    thread_ids: List[str],
) -> str:
    """
    Retrieve the content of multiple Gmail threads in a single batch request.

    Args:
        service: Authenticated Gmail API service.
        thread_ids: List of Gmail thread IDs to retrieve.

    Returns:
        Formatted list of thread contents.
    """
    logger.info(f"[get_threads_content_batch] Thread count: {len(thread_ids)}")

    if not thread_ids:
        raise ValueError("No thread IDs provided")

    output_threads = []

    def _batch_callback(request_id, response, exception):
        results[request_id] = {"data": response, "error": exception}

    for chunk_start in range(0, len(thread_ids), GMAIL_BATCH_SIZE):
        chunk_ids = thread_ids[chunk_start : chunk_start + GMAIL_BATCH_SIZE]
        results: Dict[str, Dict] = {}

        try:
            batch = service.new_batch_http_request(callback=_batch_callback)

            for tid in chunk_ids:
                req = service.users().threads().get(userId="me", id=tid, format="full")
                batch.add(req, request_id=tid)

            batch.execute()

        except Exception as batch_error:
            logger.warning(
                f"[get_threads_content_batch] Batch API failed, falling back to sequential: {batch_error}"
            )

            def fetch_thread_with_retry(tid: str, max_retries: int = 3):
                for attempt in range(max_retries):
                    try:
                        thread = (
                            service.users()
                            .threads()
                            .get(userId="me", id=tid, format="full")
                            .execute()
                        )
                        return tid, thread, None
                    except ssl.SSLError as ssl_error:
                        if attempt < max_retries - 1:
                            delay = 2 ** attempt
                            logger.warning(
                                f"[get_threads_content_batch] SSL error for {tid}, attempt {attempt + 1}: {ssl_error}. Retrying in {delay}s..."
                            )
                            time.sleep(delay)
                        else:
                            return tid, None, ssl_error
                    except Exception as e:
                        return tid, None, e

            for tid in chunk_ids:
                tid_result, thread_data, err = fetch_thread_with_retry(tid)
                results[tid_result] = {"data": thread_data, "error": err}
                time.sleep(GMAIL_REQUEST_DELAY)

        for tid in chunk_ids:
            entry = results.get(tid, {"data": None, "error": "No result"})

            if entry["error"]:
                output_threads.append(f"Thread {tid}: {entry['error']}\n")
            else:
                thread = entry["data"]
                if not thread:
                    output_threads.append(f"Thread {tid}: No data returned\n")
                    continue
                output_threads.append(_format_thread_content(thread, tid))

    header = f"Retrieved {len(thread_ids)} threads:"
    return header + "\n\n" + "\n---\n\n".join(output_threads)


def list_labels(service) -> str:
    """
    List all labels in the user's Gmail account.

    Args:
        service: Authenticated Gmail API service.

    Returns:
        Formatted list of labels.
    """
    logger.info("[list_labels] Invoked")

    response = (
        service.users()
        .labels()
        .list(userId="me")
        .execute()
    )
    labels = response.get("labels", [])

    if not labels:
        return "No labels found."

    lines = [f"Found {len(labels)} labels:", ""]

    system_labels = []
    user_labels = []

    for label in labels:
        if label.get("type") == "system":
            system_labels.append(label)
        else:
            user_labels.append(label)

    if system_labels:
        lines.append("SYSTEM LABELS:")
        for label in system_labels:
            lines.append(f"  - {label['name']} (ID: {label['id']})")
        lines.append("")

    if user_labels:
        lines.append("USER LABELS:")
        for label in user_labels:
            lines.append(f"  - {label['name']} (ID: {label['id']})")

    return "\n".join(lines)


def manage_label(
    service,
    action: Literal["create", "update", "delete"],
    name: Optional[str] = None,
    label_id: Optional[str] = None,
    label_list_visibility: Literal["labelShow", "labelHide"] = "labelShow",
    message_list_visibility: Literal["show", "hide"] = "show",
) -> str:
    """
    Manage Gmail labels: create, update, or delete.

    Args:
        service: Authenticated Gmail API service.
        action: Action to perform ('create', 'update', 'delete').
        name: Label name (required for create, optional for update).
        label_id: Label ID (required for update and delete).
        label_list_visibility: Whether label is shown in label list.
        message_list_visibility: Whether label is shown in message list.

    Returns:
        Confirmation message.
    """
    logger.info(f"[manage_label] Action: '{action}'")

    if action == "create" and not name:
        raise Exception("Label name is required for create action.")

    if action in ["update", "delete"] and not label_id:
        raise Exception("Label ID is required for update and delete actions.")

    if action == "create":
        label_object = {
            "name": name,
            "labelListVisibility": label_list_visibility,
            "messageListVisibility": message_list_visibility,
        }
        created_label = (
            service.users()
            .labels()
            .create(userId="me", body=label_object)
            .execute()
        )
        return f"Label created successfully!\nName: {created_label['name']}\nID: {created_label['id']}"

    elif action == "update":
        current_label = (
            service.users()
            .labels()
            .get(userId="me", id=label_id)
            .execute()
        )

        label_object = {
            "id": label_id,
            "name": name if name is not None else current_label["name"],
            "labelListVisibility": label_list_visibility,
            "messageListVisibility": message_list_visibility,
        }

        updated_label = (
            service.users()
            .labels()
            .update(userId="me", id=label_id, body=label_object)
            .execute()
        )
        return f"Label updated successfully!\nName: {updated_label['name']}\nID: {updated_label['id']}"

    elif action == "delete":
        label = (
            service.users()
            .labels()
            .get(userId="me", id=label_id)
            .execute()
        )
        label_name = label["name"]

        (
            service.users()
            .labels()
            .delete(userId="me", id=label_id)
            .execute()
        )
        return f"Label '{label_name}' (ID: {label_id}) deleted successfully!"


def modify_message_labels(
    service,
    message_ids: List[str],
    add_label_ids: Optional[List[str]] = None,
    remove_label_ids: Optional[List[str]] = None,
) -> str:
    """
    Add or remove labels from one or more Gmail messages.

    Args:
        service: Authenticated Gmail API service.
        message_ids: List of message IDs to modify.
        add_label_ids: Label IDs to add.
        remove_label_ids: Label IDs to remove.

    Returns:
        Confirmation message.
    """
    add_label_ids = add_label_ids or []
    remove_label_ids = remove_label_ids or []

    logger.info(f"[modify_message_labels] Message IDs: {message_ids}")

    if not add_label_ids and not remove_label_ids:
        raise Exception(
            "At least one of add_label_ids or remove_label_ids must be provided."
        )

    if len(message_ids) == 1:
        # Single message modify
        body = {}
        if add_label_ids:
            body["addLabelIds"] = add_label_ids
        if remove_label_ids:
            body["removeLabelIds"] = remove_label_ids

        (
            service.users()
            .messages()
            .modify(userId="me", id=message_ids[0], body=body)
            .execute()
        )
    else:
        # Batch modify
        body = {"ids": message_ids}
        if add_label_ids:
            body["addLabelIds"] = add_label_ids
        if remove_label_ids:
            body["removeLabelIds"] = remove_label_ids

        (
            service.users()
            .messages()
            .batchModify(userId="me", body=body)
            .execute()
        )

    actions = []
    if add_label_ids:
        actions.append(f"Added labels: {', '.join(add_label_ids)}")
    if remove_label_ids:
        actions.append(f"Removed labels: {', '.join(remove_label_ids)}")

    return f"Labels updated for {len(message_ids)} message(s): {'; '.join(actions)}"


def list_filters(service) -> str:
    """
    List all Gmail filters configured in the user's mailbox.

    Args:
        service: Authenticated Gmail API service.

    Returns:
        Formatted list of filters.
    """
    logger.info("[list_filters] Invoked")

    response = (
        service.users()
        .settings()
        .filters()
        .list(userId="me")
        .execute()
    )

    filters = response.get("filter") or response.get("filters") or []

    if not filters:
        return "No filters found."

    lines = [f"Found {len(filters)} filters:", ""]

    for filter_obj in filters:
        filter_id = filter_obj.get("id", "(no id)")
        criteria = filter_obj.get("criteria", {})
        action = filter_obj.get("action", {})

        lines.append(f"Filter ID: {filter_id}")
        lines.append("  Criteria:")

        criteria_lines = []
        if criteria.get("from"):
            criteria_lines.append(f"From: {criteria['from']}")
        if criteria.get("to"):
            criteria_lines.append(f"To: {criteria['to']}")
        if criteria.get("subject"):
            criteria_lines.append(f"Subject: {criteria['subject']}")
        if criteria.get("query"):
            criteria_lines.append(f"Query: {criteria['query']}")
        if criteria.get("negatedQuery"):
            criteria_lines.append(f"Exclude Query: {criteria['negatedQuery']}")
        if criteria.get("hasAttachment"):
            criteria_lines.append("Has attachment")
        if criteria.get("excludeChats"):
            criteria_lines.append("Exclude chats")
        if criteria.get("size"):
            comparison = criteria.get("sizeComparison", "")
            criteria_lines.append(
                f"Size {comparison or ''} {criteria['size']} bytes".strip()
            )

        if not criteria_lines:
            criteria_lines.append("(none)")

        lines.extend([f"    - {line}" for line in criteria_lines])

        lines.append("  Actions:")
        action_lines = []
        if action.get("forward"):
            action_lines.append(f"Forward to: {action['forward']}")
        if action.get("removeLabelIds"):
            action_lines.append(f"Remove labels: {', '.join(action['removeLabelIds'])}")
        if action.get("addLabelIds"):
            action_lines.append(f"Add labels: {', '.join(action['addLabelIds'])}")

        if not action_lines:
            action_lines.append("(none)")

        lines.extend([f"    - {line}" for line in action_lines])
        lines.append("")

    return "\n".join(lines).rstrip()


def create_filter(service, criteria: dict, actions: dict) -> str:
    """
    Create a Gmail filter.

    Args:
        service: Authenticated Gmail API service.
        criteria: Filter criteria dict (e.g. {"from": "user@example.com", "query": "subject:important"}).
        actions: Filter actions dict (e.g. {"addLabelIds": ["IMPORTANT"], "removeLabelIds": ["INBOX"]}).

    Returns:
        Confirmation message with filter ID.
    """
    logger.info(f"[create_filter] Criteria: {criteria}")

    filter_body = {
        "criteria": criteria,
        "action": actions,
    }

    created_filter = (
        service.users()
        .settings()
        .filters()
        .create(userId="me", body=filter_body)
        .execute()
    )

    filter_id = created_filter.get("id")
    return f"Filter created! Filter ID: {filter_id}"


def delete_filter(service, filter_id: str) -> str:
    """
    Delete a Gmail filter.

    Args:
        service: Authenticated Gmail API service.
        filter_id: The ID of the filter to delete.

    Returns:
        Confirmation message.
    """
    logger.info(f"[delete_filter] Filter ID: '{filter_id}'")

    (
        service.users()
        .settings()
        .filters()
        .delete(userId="me", id=filter_id)
        .execute()
    )

    return f"Filter '{filter_id}' deleted successfully!"


def list_drafts(service, max_results: int = 25) -> str:
    """
    List drafts in the user's Gmail account.

    Args:
        service: Authenticated Gmail API service.
        max_results: Maximum number of drafts to return.

    Returns:
        Formatted list of drafts with IDs, subjects, and recipients.
    """
    logger.info(f"[list_drafts] max_results={max_results}")

    response = (
        service.users()
        .drafts()
        .list(userId="me", maxResults=max_results)
        .execute()
    )

    drafts = response.get("drafts", [])
    if not drafts:
        return "No drafts found."

    output = [f"Found {len(drafts)} drafts:\n"]

    for draft in drafts:
        draft_id = draft.get("id", "")
        message = draft.get("message", {})
        message_id = message.get("id", "")

        # Fetch draft details for subject/to
        detail = (
            service.users()
            .drafts()
            .get(userId="me", id=draft_id, format="metadata")
            .execute()
        )
        msg = detail.get("message", {})
        payload = msg.get("payload", {})
        headers = _extract_headers(payload, ["Subject", "To", "Date"])

        subject = headers.get("Subject", "(no subject)")
        to = headers.get("To", "(no recipient)")
        date = headers.get("Date", "")

        output.append(f"  Draft ID: {draft_id}")
        output.append(f"  Message ID: {message_id}")
        output.append(f"  Subject: {subject}")
        output.append(f"  To: {to}")
        if date:
            output.append(f"  Date: {date}")
        output.append("")

    return "\n".join(output)


def get_draft(service, draft_id: str, html: bool = False) -> str:
    """
    Get the full content of a draft.

    Args:
        service: Authenticated Gmail API service.
        draft_id: The ID of the draft.

    Returns:
        Formatted draft content with headers, body, and attachment info.
    """
    logger.info(f"[get_draft] Draft ID: '{draft_id}'")

    draft = (
        service.users()
        .drafts()
        .get(userId="me", id=draft_id, format="full")
        .execute()
    )

    msg = draft.get("message", {})
    payload = msg.get("payload", {})
    headers = _extract_headers(
        payload, ["Subject", "To", "From", "Cc", "Bcc", "Date", "In-Reply-To"]
    )
    thread_id = msg.get("threadId", "")

    bodies = _extract_message_bodies(payload)
    attachments = _extract_attachments(payload)

    output = [
        f"Draft ID: {draft_id}",
        f"Message ID: {msg.get('id', '')}",
        f"Thread ID: {thread_id}",
    ]
    for key, val in headers.items():
        output.append(f"{key}: {val}")
    output.append("")

    if html:
        body_text = bodies.get("html") or bodies.get("text", "(empty body)")
    else:
        body_text = _format_body_content(bodies.get("text", ""), bodies.get("html", "")) or "(empty body)"
    output.append(f"Body:\n{body_text}")

    if attachments:
        output.append(f"\nAttachments ({len(attachments)}):")
        for att in attachments:
            output.append(
                f"  - {att.get('filename', 'unnamed')} "
                f"({att.get('mimeType', 'unknown')}, {att.get('size', 0)} bytes)"
            )

    return "\n".join(output)


def update_draft(
    service,
    draft_id: str,
    subject: Optional[str] = None,
    body: Optional[str] = None,
    to: Optional[str] = None,
    cc: Optional[str] = None,
    bcc: Optional[str] = None,
    body_format: Literal["plain", "html"] = "plain",
    attachment_paths: Optional[list[str]] = None,
) -> str:
    """
    Update an existing draft. Replaces the draft message entirely.

    If subject/body/to are not provided, they are preserved from the
    existing draft. New attachment_paths replace all existing attachments.

    Args:
        service: Authenticated Gmail API service.
        draft_id: The ID of the draft to update.
        subject: New subject (or None to keep existing).
        body: New body content (or None to keep existing).
        to: New recipient (or None to keep existing).
        cc: New CC (or None to keep existing).
        bcc: New BCC (or None to keep existing).
        body_format: 'plain' or 'html'.
        attachment_paths: List of file paths to attach (replaces existing).

    Returns:
        Confirmation message.
    """
    logger.info(f"[update_draft] Draft ID: '{draft_id}'")

    # Fetch existing draft to preserve fields not being updated
    existing = (
        service.users()
        .drafts()
        .get(userId="me", id=draft_id, format="full")
        .execute()
    )
    existing_msg = existing.get("message", {})
    existing_payload = existing_msg.get("payload", {})
    existing_headers = _extract_headers(
        existing_payload, ["Subject", "To", "Cc", "Bcc", "In-Reply-To", "References"]
    )
    existing_bodies = _extract_message_bodies(existing_payload)
    existing_thread_id = existing_msg.get("threadId")

    # Use provided values or fall back to existing
    final_subject = subject if subject is not None else existing_headers.get("Subject", "")
    final_to = to if to is not None else existing_headers.get("To")
    final_cc = cc if cc is not None else existing_headers.get("Cc")
    final_bcc = bcc if bcc is not None else existing_headers.get("Bcc")
    final_body = body if body is not None else (
        existing_bodies.get("text/plain") or existing_bodies.get("text/html", "")
    )

    raw_message, _ = _prepare_gmail_message(
        subject=final_subject,
        body=final_body,
        body_format=body_format,
        to=final_to,
        cc=final_cc,
        bcc=final_bcc,
        thread_id=existing_thread_id,
        in_reply_to=existing_headers.get("In-Reply-To"),
        references=existing_headers.get("References"),
        attachment_paths=attachment_paths,
    )

    draft_body = {"message": {"raw": raw_message}}
    if existing_thread_id:
        draft_body["message"]["threadId"] = existing_thread_id

    updated = (
        service.users()
        .drafts()
        .update(userId="me", id=draft_id, body=draft_body)
        .execute()
    )

    return f"Draft updated! Draft ID: {updated.get('id')}"


def delete_draft(service, draft_id: str) -> str:
    """
    Delete a draft.

    Args:
        service: Authenticated Gmail API service.
        draft_id: The ID of the draft to delete.

    Returns:
        Confirmation message.
    """
    logger.info(f"[delete_draft] Draft ID: '{draft_id}'")

    (
        service.users()
        .drafts()
        .delete(userId="me", id=draft_id)
        .execute()
    )

    return f"Draft '{draft_id}' deleted."


def send_draft(service, draft_id: str) -> str:
    """
    Send an existing draft.

    Args:
        service: Authenticated Gmail API service.
        draft_id: The ID of the draft to send.

    Returns:
        Confirmation message with sent message ID.
    """
    logger.info(f"[send_draft] Draft ID: '{draft_id}'")

    sent = (
        service.users()
        .drafts()
        .send(userId="me", body={"id": draft_id})
        .execute()
    )

    message_id = sent.get("id", "")
    return f"Draft sent! Message ID: {message_id}"
