"""
Google Forms service functions for the gw CLI.

Ported from gforms/forms_tools.py with MCP decorators removed.
"""

import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


def create_form(
    service,
    title: str,
    description: Optional[str] = None,
    document_title: Optional[str] = None,
) -> str:
    """Create a new Google Form."""
    logger.info(f"[create_form] Invoked. Title: {title}")

    form_body: Dict[str, Any] = {"info": {"title": title}}

    if description:
        form_body["info"]["description"] = description

    if document_title:
        form_body["info"]["document_title"] = document_title

    created_form = service.forms().create(body=form_body).execute()

    form_id = created_form.get("formId")
    edit_url = f"https://docs.google.com/forms/d/{form_id}/edit"
    responder_url = created_form.get(
        "responderUri", f"https://docs.google.com/forms/d/{form_id}/viewform"
    )

    confirmation_message = (
        f"Successfully created form '{created_form.get('info', {}).get('title', title)}'. "
        f"Form ID: {form_id}. Edit URL: {edit_url}. Responder URL: {responder_url}"
    )
    logger.info(f"Form created successfully. ID: {form_id}")
    return confirmation_message


def get_form(service, form_id: str) -> str:
    """Get a form's details."""
    logger.info(f"[get_form] Invoked. Form ID: {form_id}")

    form = service.forms().get(formId=form_id).execute()

    form_info = form.get("info", {})
    title = form_info.get("title", "No Title")
    description = form_info.get("description", "No Description")
    document_title = form_info.get("documentTitle", title)

    edit_url = f"https://docs.google.com/forms/d/{form_id}/edit"
    responder_url = form.get(
        "responderUri", f"https://docs.google.com/forms/d/{form_id}/viewform"
    )

    items = form.get("items", [])
    questions_summary = []
    for i, item in enumerate(items, 1):
        item_title = item.get("title", f"Question {i}")
        item_type = (
            item.get("questionItem", {}).get("question", {}).get("required", False)
        )
        required_text = " (Required)" if item_type else ""
        questions_summary.append(f"  {i}. {item_title}{required_text}")

    questions_text = (
        "\n".join(questions_summary) if questions_summary else "  No questions found"
    )

    result = f"""Form Details:
- Title: "{title}"
- Description: "{description}"
- Document Title: "{document_title}"
- Form ID: {form_id}
- Edit URL: {edit_url}
- Responder URL: {responder_url}
- Questions ({len(items)} total):
{questions_text}"""

    logger.info(f"Successfully retrieved form. ID: {form_id}")
    return result


def set_publish_settings(
    service,
    form_id: str,
    publish_as_template: bool = False,
    require_authentication: bool = False,
) -> str:
    """Update the publish settings of a form."""
    logger.info(f"[set_publish_settings] Invoked. Form ID: {form_id}")

    settings_body = {
        "publishAsTemplate": publish_as_template,
        "requireAuthentication": require_authentication,
    }

    (
        service.forms()
        .setPublishSettings(formId=form_id, body=settings_body)
        .execute()
    )

    confirmation_message = (
        f"Successfully updated publish settings for form {form_id}. "
        f"Publish as template: {publish_as_template}, "
        f"Require authentication: {require_authentication}"
    )
    logger.info(f"Publish settings updated successfully. Form ID: {form_id}")
    return confirmation_message


def get_form_response(service, form_id: str, response_id: str) -> str:
    """Get one response from the form."""
    logger.info(
        f"[get_form_response] Invoked. Form ID: {form_id}, Response ID: {response_id}"
    )

    response = (
        service.forms()
        .responses()
        .get(formId=form_id, responseId=response_id)
        .execute()
    )

    response_id = response.get("responseId", "Unknown")
    create_time = response.get("createTime", "Unknown")
    last_submitted_time = response.get("lastSubmittedTime", "Unknown")

    answers = response.get("answers", {})
    answer_details = []
    for question_id, answer_data in answers.items():
        question_response = answer_data.get("textAnswers", {}).get("answers", [])
        if question_response:
            answer_text = ", ".join([ans.get("value", "") for ans in question_response])
            answer_details.append(f"  Question ID {question_id}: {answer_text}")
        else:
            answer_details.append(f"  Question ID {question_id}: No answer provided")

    answers_text = "\n".join(answer_details) if answer_details else "  No answers found"

    result = f"""Form Response Details:
- Form ID: {form_id}
- Response ID: {response_id}
- Created: {create_time}
- Last Submitted: {last_submitted_time}
- Answers:
{answers_text}"""

    logger.info(f"Successfully retrieved response. Response ID: {response_id}")
    return result


def list_form_responses(
    service,
    form_id: str,
    page_size: int = 10,
    page_token: Optional[str] = None,
) -> str:
    """List a form's responses."""
    logger.info(f"[list_form_responses] Invoked. Form ID: {form_id}")

    params = {"formId": form_id, "pageSize": page_size}
    if page_token:
        params["pageToken"] = page_token

    responses_result = (
        service.forms()
        .responses()
        .list(**params)
        .execute()
    )

    responses = responses_result.get("responses", [])
    next_page_token = responses_result.get("nextPageToken")

    if not responses:
        return f"No responses found for form {form_id}."

    response_details = []
    for i, response in enumerate(responses, 1):
        response_id = response.get("responseId", "Unknown")
        create_time = response.get("createTime", "Unknown")
        last_submitted_time = response.get("lastSubmittedTime", "Unknown")

        answers_count = len(response.get("answers", {}))
        response_details.append(
            f"  {i}. Response ID: {response_id} | Created: {create_time} | "
            f"Last Submitted: {last_submitted_time} | Answers: {answers_count}"
        )

    pagination_info = (
        f"\nNext page token: {next_page_token}"
        if next_page_token
        else "\nNo more pages."
    )

    result = f"""Form Responses:
- Form ID: {form_id}
- Total responses returned: {len(responses)}
- Responses:
{chr(10).join(response_details)}{pagination_info}"""

    logger.info(f"Successfully retrieved {len(responses)} responses. Form ID: {form_id}")
    return result
