"""
Google Calendar service functions for the gw CLI.

Ported from gcalendar/calendar_tools.py with MCP decorators removed.
"""

import datetime
import json
import logging
import re
import uuid
from typing import Any, Dict, List, Optional, Union

from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_reminders_json(
    reminders_input: Optional[Union[str, List[Dict[str, Any]]]], function_name: str
) -> List[Dict[str, Any]]:
    """Parse reminders from JSON string or list object and validate them."""
    if not reminders_input:
        return []

    if isinstance(reminders_input, str):
        try:
            reminders = json.loads(reminders_input)
            if not isinstance(reminders, list):
                logger.warning(
                    f"[{function_name}] Reminders must be a JSON array, got {type(reminders).__name__}"
                )
                return []
        except json.JSONDecodeError as e:
            logger.warning(f"[{function_name}] Invalid JSON for reminders: {e}")
            return []
    elif isinstance(reminders_input, list):
        reminders = reminders_input
    else:
        logger.warning(
            f"[{function_name}] Reminders must be a JSON string or list, got {type(reminders_input).__name__}"
        )
        return []

    if len(reminders) > 5:
        logger.warning(
            f"[{function_name}] More than 5 reminders provided, truncating to first 5"
        )
        reminders = reminders[:5]

    validated_reminders = []
    for reminder in reminders:
        if (
            not isinstance(reminder, dict)
            or "method" not in reminder
            or "minutes" not in reminder
        ):
            logger.warning(
                f"[{function_name}] Invalid reminder format: {reminder}, skipping"
            )
            continue

        method = reminder["method"].lower()
        if method not in ["popup", "email"]:
            logger.warning(
                f"[{function_name}] Invalid reminder method '{method}', must be 'popup' or 'email', skipping"
            )
            continue

        minutes = reminder["minutes"]
        if not isinstance(minutes, int) or minutes < 0 or minutes > 40320:
            logger.warning(
                f"[{function_name}] Invalid reminder minutes '{minutes}', must be integer 0-40320, skipping"
            )
            continue

        validated_reminders.append({"method": method, "minutes": minutes})

    return validated_reminders


def _apply_transparency_if_valid(
    event_body: Dict[str, Any],
    transparency: Optional[str],
    function_name: str,
) -> None:
    """Apply transparency to the event body if the provided value is valid."""
    if transparency is None:
        return
    valid = ["opaque", "transparent"]
    if transparency in valid:
        event_body["transparency"] = transparency
        logger.info(f"[{function_name}] Set transparency to '{transparency}'")
    else:
        logger.warning(
            f"[{function_name}] Invalid transparency value '{transparency}', must be 'opaque' or 'transparent', skipping"
        )


def _apply_visibility_if_valid(
    event_body: Dict[str, Any],
    visibility: Optional[str],
    function_name: str,
) -> None:
    """Apply visibility to the event body if the provided value is valid."""
    if visibility is None:
        return
    valid = ["default", "public", "private", "confidential"]
    if visibility in valid:
        event_body["visibility"] = visibility
        logger.info(f"[{function_name}] Set visibility to '{visibility}'")
    else:
        logger.warning(
            f"[{function_name}] Invalid visibility value '{visibility}', must be one of {valid}, skipping"
        )


def _preserve_existing_fields(
    event_body: Dict[str, Any],
    existing_event: Dict[str, Any],
    field_mappings: Dict[str, Any],
) -> None:
    """Preserve existing event fields when not explicitly provided."""
    for field_name, new_value in field_mappings.items():
        if new_value is None and field_name in existing_event:
            event_body[field_name] = existing_event[field_name]
            logger.info(f"[modify_event] Preserving existing {field_name}")
        elif new_value is not None:
            event_body[field_name] = new_value


def _format_attendee_details(
    attendees: List[Dict[str, Any]], indent: str = "  "
) -> str:
    """Format attendee details including response status, organizer, and optional flags."""
    if not attendees:
        return "None"

    attendee_details_list = []
    for a in attendees:
        email = a.get("email", "unknown")
        response_status = a.get("responseStatus", "unknown")
        optional = a.get("optional", False)
        organizer = a.get("organizer", False)

        detail_parts = [f"{email}: {response_status}"]
        if organizer:
            detail_parts.append("(organizer)")
        if optional:
            detail_parts.append("(optional)")

        attendee_details_list.append(" ".join(detail_parts))

    return f"\n{indent}".join(attendee_details_list)


def _format_attachment_details(
    attachments: List[Dict[str, Any]], indent: str = "  "
) -> str:
    """Format attachment details including file information."""
    if not attachments:
        return "None"

    attachment_details_list = []
    for att in attachments:
        title = att.get("title", "Untitled")
        file_url = att.get("fileUrl", "No URL")
        file_id = att.get("fileId", "No ID")
        mime_type = att.get("mimeType", "Unknown")

        attachment_info = (
            f"{title}\n"
            f"{indent}File URL: {file_url}\n"
            f"{indent}File ID: {file_id}\n"
            f"{indent}MIME Type: {mime_type}"
        )
        attachment_details_list.append(attachment_info)

    return f"\n{indent}".join(attachment_details_list)


def _correct_time_format_for_api(
    time_str: Optional[str], param_name: str
) -> Optional[str]:
    """Ensure time strings for API calls are correctly formatted."""
    if not time_str:
        return None

    logger.info(
        f"_correct_time_format_for_api: Processing {param_name} with value '{time_str}'"
    )

    # Handle date-only format (YYYY-MM-DD)
    if len(time_str) == 10 and time_str.count("-") == 2:
        try:
            datetime.datetime.strptime(time_str, "%Y-%m-%d")
            formatted = f"{time_str}T00:00:00Z"
            logger.info(
                f"Formatting date-only {param_name} '{time_str}' to RFC3339: '{formatted}'"
            )
            return formatted
        except ValueError:
            logger.warning(
                f"{param_name} '{time_str}' looks like a date but is not valid YYYY-MM-DD. Using as is."
            )
            return time_str

    # Specifically address YYYY-MM-DDTHH:MM:SS by appending 'Z'
    if (
        len(time_str) == 19
        and time_str[10] == "T"
        and time_str.count(":") == 2
        and not (
            time_str.endswith("Z") or ("+" in time_str[10:]) or ("-" in time_str[10:])
        )
    ):
        try:
            datetime.datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S")
            logger.info(
                f"Formatting {param_name} '{time_str}' by appending 'Z' for UTC."
            )
            return time_str + "Z"
        except ValueError:
            logger.warning(
                f"{param_name} '{time_str}' looks like it needs 'Z' but is not valid. Using as is."
            )
            return time_str

    logger.info(f"{param_name} '{time_str}' doesn't need formatting, using as is.")
    return time_str


# ---------------------------------------------------------------------------
# Service functions
# ---------------------------------------------------------------------------

def list_calendars(service) -> str:
    """Retrieve a list of calendars accessible to the authenticated user."""
    logger.info("[list_calendars] Invoked.")

    calendar_list_response = service.calendarList().list().execute()
    items = calendar_list_response.get("items", [])
    if not items:
        return "No calendars found."

    calendars_summary_list = [
        f'- "{cal.get("summary", "No Summary")}"{" (Primary)" if cal.get("primary") else ""} (ID: {cal["id"]})'
        for cal in items
    ]
    text_output = (
        f"Successfully listed {len(items)} calendars:\n"
        + "\n".join(calendars_summary_list)
    )
    logger.info(f"Successfully listed {len(items)} calendars.")
    return text_output


def get_events(
    service,
    calendar_id: str = "primary",
    event_id: Optional[str] = None,
    time_min: Optional[str] = None,
    time_max: Optional[str] = None,
    max_results: int = 25,
    query: Optional[str] = None,
    detailed: bool = False,
    include_attachments: bool = False,
) -> str:
    """Retrieve events from a specified Google Calendar."""
    logger.info(
        f"[get_events] Raw parameters - event_id: '{event_id}', time_min: '{time_min}', "
        f"time_max: '{time_max}', query: '{query}', detailed: {detailed}, "
        f"include_attachments: {include_attachments}"
    )

    # Handle single event retrieval
    if event_id:
        logger.info(f"[get_events] Retrieving single event with ID: {event_id}")
        event = (
            service.events()
            .get(calendarId=calendar_id, eventId=event_id)
            .execute()
        )
        items = [event]
    else:
        formatted_time_min = _correct_time_format_for_api(time_min, "time_min")
        if formatted_time_min:
            effective_time_min = formatted_time_min
        else:
            utc_now = datetime.datetime.now(datetime.timezone.utc)
            effective_time_min = utc_now.isoformat().replace("+00:00", "Z")

        effective_time_max = _correct_time_format_for_api(time_max, "time_max")

        logger.info(
            f"[get_events] Final API parameters - calendarId: '{calendar_id}', "
            f"timeMin: '{effective_time_min}', timeMax: '{effective_time_max}', "
            f"maxResults: {max_results}, query: '{query}'"
        )

        request_params = {
            "calendarId": calendar_id,
            "timeMin": effective_time_min,
            "timeMax": effective_time_max,
            "maxResults": max_results,
            "singleEvents": True,
            "orderBy": "startTime",
        }

        if query:
            request_params["q"] = query

        events_result = service.events().list(**request_params).execute()
        items = events_result.get("items", [])

    if not items:
        if event_id:
            return f"Event with ID '{event_id}' not found in calendar '{calendar_id}'."
        else:
            return f"No events found in calendar '{calendar_id}' for the specified time range."

    # Handle returning detailed output for a single event when requested
    if event_id and detailed:
        item = items[0]
        summary = item.get("summary", "No Title")
        start = item["start"].get("dateTime", item["start"].get("date"))
        end = item["end"].get("dateTime", item["end"].get("date"))
        link = item.get("htmlLink", "No Link")
        description = item.get("description", "No Description")
        location = item.get("location", "No Location")
        color_id = item.get("colorId", "None")
        attendees = item.get("attendees", [])
        attendee_emails = (
            ", ".join([a.get("email", "") for a in attendees]) if attendees else "None"
        )
        attendee_details_str = _format_attendee_details(attendees, indent="  ")

        event_details = (
            f"Event Details:\n"
            f"- Title: {summary}\n"
            f"- Starts: {start}\n"
            f"- Ends: {end}\n"
            f"- Description: {description}\n"
            f"- Location: {location}\n"
            f"- Color ID: {color_id}\n"
            f"- Attendees: {attendee_emails}\n"
            f"- Attendee Details: {attendee_details_str}\n"
        )

        if include_attachments:
            attachments = item.get("attachments", [])
            attachment_details_str = _format_attachment_details(attachments, indent="  ")
            event_details += f"- Attachments: {attachment_details_str}\n"

        event_details += f"- Event ID: {event_id}\n- Link: {link}"
        logger.info(f"[get_events] Successfully retrieved detailed event {event_id}.")
        return event_details

    # Handle multiple events or single event with basic output
    event_details_list = []
    for item in items:
        summary = item.get("summary", "No Title")
        start_time = item["start"].get("dateTime", item["start"].get("date"))
        end_time = item["end"].get("dateTime", item["end"].get("date"))
        link = item.get("htmlLink", "No Link")
        item_event_id = item.get("id", "No ID")

        if detailed:
            description = item.get("description", "No Description")
            location = item.get("location", "No Location")
            attendees = item.get("attendees", [])
            attendee_emails = (
                ", ".join([a.get("email", "") for a in attendees])
                if attendees
                else "None"
            )
            attendee_details_str = _format_attendee_details(attendees, indent="    ")

            event_detail_parts = (
                f'- "{summary}" (Starts: {start_time}, Ends: {end_time})\n'
                f"  Description: {description}\n"
                f"  Location: {location}\n"
                f"  Attendees: {attendee_emails}\n"
                f"  Attendee Details: {attendee_details_str}\n"
            )

            if include_attachments:
                attachments = item.get("attachments", [])
                attachment_details_str = _format_attachment_details(
                    attachments, indent="    "
                )
                event_detail_parts += f"  Attachments: {attachment_details_str}\n"

            event_detail_parts += f"  ID: {item_event_id} | Link: {link}"
            event_details_list.append(event_detail_parts)
        else:
            event_details_list.append(
                f'- "{summary}" (Starts: {start_time}, Ends: {end_time}) ID: {item_event_id} | Link: {link}'
            )

    if event_id:
        text_output = (
            f"Successfully retrieved event from calendar '{calendar_id}':\n"
            + "\n".join(event_details_list)
        )
    else:
        text_output = (
            f"Successfully retrieved {len(items)} events from calendar '{calendar_id}':\n"
            + "\n".join(event_details_list)
        )

    logger.info(f"Successfully retrieved {len(items)} events.")
    return text_output


def create_event(
    service,
    summary: str,
    start_time: str,
    end_time: str,
    calendar_id: str = "primary",
    description: Optional[str] = None,
    location: Optional[str] = None,
    timezone: Optional[str] = None,
    attendees: Optional[List[str]] = None,
    add_google_meet: bool = False,
    reminders: Optional[Union[str, List[Dict[str, Any]]]] = None,
    use_default_reminders: bool = True,
    recurrence: Optional[List[str]] = None,
    transparency: Optional[str] = None,
    visibility: Optional[str] = None,
) -> str:
    """Create a new calendar event."""
    logger.info(f"[create_event] Invoked. Summary: {summary}")

    event_body: Dict[str, Any] = {
        "summary": summary,
        "start": (
            {"date": start_time} if "T" not in start_time else {"dateTime": start_time}
        ),
        "end": ({"date": end_time} if "T" not in end_time else {"dateTime": end_time}),
    }
    if location:
        event_body["location"] = location
    if description:
        event_body["description"] = description
    if timezone:
        if "dateTime" in event_body["start"]:
            event_body["start"]["timeZone"] = timezone
        if "dateTime" in event_body["end"]:
            event_body["end"]["timeZone"] = timezone

    # Attendees
    if attendees:
        event_body["attendees"] = [{"email": addr} for addr in attendees]

    # Recurrence
    if recurrence:
        event_body["recurrence"] = recurrence

    # Reminders
    if reminders is not None or not use_default_reminders:
        effective_use_default = use_default_reminders and reminders is None
        reminder_data = {"useDefault": effective_use_default}
        if reminders is not None:
            validated_reminders = _parse_reminders_json(reminders, "create_event")
            if validated_reminders:
                reminder_data["overrides"] = validated_reminders
                logger.info(
                    f"[create_event] Added {len(validated_reminders)} custom reminders"
                )
        event_body["reminders"] = reminder_data

    _apply_transparency_if_valid(event_body, transparency, "create_event")
    _apply_visibility_if_valid(event_body, visibility, "create_event")

    if add_google_meet:
        request_id = str(uuid.uuid4())
        event_body["conferenceData"] = {
            "createRequest": {
                "requestId": request_id,
                "conferenceSolutionKey": {"type": "hangoutsMeet"},
            }
        }
        logger.info(
            f"[create_event] Adding Google Meet conference with request ID: {request_id}"
        )

    created_event = (
        service.events()
        .insert(
            calendarId=calendar_id,
            body=event_body,
            conferenceDataVersion=1 if add_google_meet else 0,
        )
        .execute()
    )

    link = created_event.get("htmlLink", "No link available")
    confirmation_message = f"Successfully created event '{created_event.get('summary', summary)}'. Link: {link}"

    if add_google_meet and "conferenceData" in created_event:
        conference_data = created_event["conferenceData"]
        if "entryPoints" in conference_data:
            for entry_point in conference_data["entryPoints"]:
                if entry_point.get("entryPointType") == "video":
                    meet_link = entry_point.get("uri", "")
                    if meet_link:
                        confirmation_message += f" Google Meet: {meet_link}"
                        break

    logger.info(
        f"Event created successfully. ID: {created_event.get('id')}, Link: {link}"
    )
    return confirmation_message


def modify_event(
    service,
    event_id: str,
    calendar_id: str = "primary",
    summary: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    description: Optional[str] = None,
    location: Optional[str] = None,
    timezone: Optional[str] = None,
    attendees: Optional[List[str]] = None,
    add_google_meet: Optional[bool] = None,
    reminders: Optional[Union[str, List[Dict[str, Any]]]] = None,
    use_default_reminders: Optional[bool] = None,
    transparency: Optional[str] = None,
    visibility: Optional[str] = None,
    color_id: Optional[str] = None,
) -> str:
    """Modify an existing calendar event."""
    logger.info(f"[modify_event] Invoked. Event ID: {event_id}")

    event_body: Dict[str, Any] = {}
    if summary is not None:
        event_body["summary"] = summary
    if start_time is not None:
        event_body["start"] = (
            {"date": start_time} if "T" not in start_time else {"dateTime": start_time}
        )
        if timezone is not None and "dateTime" in event_body["start"]:
            event_body["start"]["timeZone"] = timezone
    if end_time is not None:
        event_body["end"] = (
            {"date": end_time} if "T" not in end_time else {"dateTime": end_time}
        )
        if timezone is not None and "dateTime" in event_body["end"]:
            event_body["end"]["timeZone"] = timezone
    if description is not None:
        event_body["description"] = description
    if location is not None:
        event_body["location"] = location
    if color_id is not None:
        event_body["colorId"] = color_id

    # Attendees
    if attendees is not None:
        event_body["attendees"] = [{"email": addr} for addr in attendees]

    # Reminders
    if reminders is not None or use_default_reminders is not None:
        reminder_data = {}
        if use_default_reminders is not None:
            reminder_data["useDefault"] = use_default_reminders
        else:
            try:
                existing_event = (
                    service.events()
                    .get(calendarId=calendar_id, eventId=event_id)
                    .execute()
                )
                reminder_data["useDefault"] = existing_event.get("reminders", {}).get(
                    "useDefault", True
                )
            except Exception as e:
                logger.warning(
                    f"[modify_event] Could not fetch existing event for reminders: {e}"
                )
                reminder_data["useDefault"] = True

        if reminders is not None:
            if reminder_data.get("useDefault", False):
                reminder_data["useDefault"] = False
                logger.info(
                    "[modify_event] Custom reminders provided - disabling default reminders"
                )
            validated_reminders = _parse_reminders_json(reminders, "modify_event")
            if reminders and not validated_reminders:
                logger.warning(
                    "[modify_event] Reminders provided but failed validation."
                )
            elif validated_reminders:
                reminder_data["overrides"] = validated_reminders
                logger.info(
                    f"[modify_event] Updated reminders with {len(validated_reminders)} custom reminders"
                )

        event_body["reminders"] = reminder_data

    _apply_transparency_if_valid(event_body, transparency, "modify_event")
    _apply_visibility_if_valid(event_body, visibility, "modify_event")

    if timezone is not None and "start" not in event_body and "end" not in event_body:
        logger.warning(
            "[modify_event] Timezone provided but start_time and end_time are missing. "
            "Timezone will not be applied."
        )

    if not event_body:
        message = "No fields provided to modify the event."
        logger.warning(f"[modify_event] {message}")
        raise Exception(message)

    # Get the existing event to preserve fields that aren't being updated
    try:
        existing_event = (
            service.events()
            .get(calendarId=calendar_id, eventId=event_id)
            .execute()
        )
        logger.info("[modify_event] Successfully retrieved existing event before update")

        _preserve_existing_fields(
            event_body,
            existing_event,
            {
                "summary": summary,
                "description": description,
                "location": location,
                "colorId": event_body.get("colorId"),
            },
        )

        # Handle Google Meet conference data
        if add_google_meet is not None:
            if add_google_meet:
                request_id = str(uuid.uuid4())
                event_body["conferenceData"] = {
                    "createRequest": {
                        "requestId": request_id,
                        "conferenceSolutionKey": {"type": "hangoutsMeet"},
                    }
                }
                logger.info(
                    f"[modify_event] Adding Google Meet conference with request ID: {request_id}"
                )
            else:
                event_body["conferenceData"] = {}
                logger.info("[modify_event] Removing Google Meet conference")
        elif "conferenceData" in existing_event:
            event_body["conferenceData"] = existing_event["conferenceData"]
            logger.info("[modify_event] Preserving existing conference data")

    except HttpError as get_error:
        if get_error.resp.status == 404:
            logger.error(
                f"[modify_event] Event not found during pre-update verification: {get_error}"
            )
            message = (
                f"Event not found. The event with ID '{event_id}' could not be found "
                f"in calendar '{calendar_id}'."
            )
            raise Exception(message)
        else:
            logger.warning(
                f"[modify_event] Error during pre-update verification, proceeding: {get_error}"
            )

    updated_event = (
        service.events()
        .update(
            calendarId=calendar_id,
            eventId=event_id,
            body=event_body,
            conferenceDataVersion=1,
        )
        .execute()
    )

    link = updated_event.get("htmlLink", "No link available")
    confirmation_message = (
        f"Successfully modified event '{updated_event.get('summary', summary)}' "
        f"(ID: {event_id}). Link: {link}"
    )

    if add_google_meet is True and "conferenceData" in updated_event:
        conference_data = updated_event["conferenceData"]
        if "entryPoints" in conference_data:
            for entry_point in conference_data["entryPoints"]:
                if entry_point.get("entryPointType") == "video":
                    meet_link = entry_point.get("uri", "")
                    if meet_link:
                        confirmation_message += f" Google Meet: {meet_link}"
                        break
    elif add_google_meet is False:
        confirmation_message += " (Google Meet removed)"

    logger.info(
        f"Event modified successfully. ID: {updated_event.get('id')}, Link: {link}"
    )
    return confirmation_message


def delete_event(
    service, event_id: str, calendar_id: str = "primary"
) -> str:
    """Delete an existing calendar event."""
    logger.info(f"[delete_event] Invoked. Event ID: {event_id}")

    # Verify event exists
    try:
        (
            service.events()
            .get(calendarId=calendar_id, eventId=event_id)
            .execute()
        )
        logger.info("[delete_event] Successfully verified event exists before deletion")
    except HttpError as get_error:
        if get_error.resp.status == 404:
            message = (
                f"Event not found. The event with ID '{event_id}' could not be found "
                f"in calendar '{calendar_id}'."
            )
            raise Exception(message)
        else:
            logger.warning(
                f"[delete_event] Error during pre-delete verification, proceeding: {get_error}"
            )

    (
        service.events()
        .delete(calendarId=calendar_id, eventId=event_id)
        .execute()
    )

    confirmation_message = (
        f"Successfully deleted event (ID: {event_id}) from calendar '{calendar_id}'."
    )
    logger.info(f"Event deleted successfully. ID: {event_id}")
    return confirmation_message
