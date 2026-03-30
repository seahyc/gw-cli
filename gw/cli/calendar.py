"""CLI commands for Google Calendar."""

from gw.auth import get_service
from gw.output import success, error
from gw.services import calendar


def register(subparsers):
    parser = subparsers.add_parser("calendar", help="Google Calendar operations")
    sub = parser.add_subparsers(dest="action", required=True)

    # calendar list
    sub.add_parser("list", help="List all calendars")

    # calendar events
    p_events = sub.add_parser("events", help="List or get events")
    p_events.add_argument("--calendar-id", default="primary")
    p_events.add_argument("--time-min", help="Start time (RFC3339 or YYYY-MM-DD)")
    p_events.add_argument("--time-max", help="End time (RFC3339 or YYYY-MM-DD)")
    p_events.add_argument("--max-results", type=int, default=25)
    p_events.add_argument("--query", help="Free-text search query")
    p_events.add_argument("--detailed", action="store_true")
    p_events.add_argument("--event-id", help="Get a single event by ID")

    # calendar create-event
    p_create = sub.add_parser("create-event", help="Create a new event")
    p_create.add_argument("--summary", required=True)
    p_create.add_argument("--start", required=True, help="Start time (RFC3339)")
    p_create.add_argument("--end", required=True, help="End time (RFC3339)")
    p_create.add_argument("--calendar-id", default="primary")
    p_create.add_argument("--description")
    p_create.add_argument("--location")
    p_create.add_argument("--timezone")
    p_create.add_argument("--attendees", nargs="+", metavar="EMAIL", help="Attendee email addresses")
    p_create.add_argument("--add-meet", action="store_true", help="Add Google Meet link")
    p_create.add_argument("--reminders", help="JSON array of reminder objects")
    p_create.add_argument("--recurrence", nargs="+", metavar="RRULE", help="Recurrence rules (e.g. RRULE:FREQ=WEEKLY;COUNT=10)")
    p_create.add_argument("--visibility", choices=["default", "public", "private", "confidential"])
    p_create.add_argument("--transparency", choices=["opaque", "transparent"])

    # calendar modify-event
    p_modify = sub.add_parser("modify-event", help="Modify an existing event")
    p_modify.add_argument("event_id")
    p_modify.add_argument("--calendar-id", default="primary")
    p_modify.add_argument("--summary")
    p_modify.add_argument("--start", help="New start time (RFC3339)")
    p_modify.add_argument("--end", help="New end time (RFC3339)")
    p_modify.add_argument("--description")
    p_modify.add_argument("--location")
    p_modify.add_argument("--attendees", nargs="+", metavar="EMAIL", help="Attendee email addresses")
    p_modify.add_argument("--add-meet", type=lambda v: v.lower() in ("true", "1", "yes"), default=None, help="Add/remove Google Meet (true/false)")
    p_modify.add_argument("--reminders", help="JSON array of reminder objects")
    p_modify.add_argument("--visibility", choices=["default", "public", "private", "confidential"])

    # calendar delete-event
    p_delete = sub.add_parser("delete-event", help="Delete an event")
    p_delete.add_argument("event_id")
    p_delete.add_argument("--calendar-id", default="primary")


def handle(args):
    action = args.action
    if action == "list":
        cmd_list(args)
    elif action == "events":
        cmd_events(args)
    elif action == "create-event":
        cmd_create_event(args)
    elif action == "modify-event":
        cmd_modify_event(args)
    elif action == "delete-event":
        cmd_delete_event(args)


def cmd_list(args):
    try:
        service = get_service("calendar")
        result = calendar.list_calendars(service)
        success(result)
    except Exception as e:
        error(str(e))


def cmd_events(args):
    try:
        service = get_service("calendar")
        result = calendar.get_events(
            service,
            calendar_id=args.calendar_id,
            event_id=args.event_id,
            time_min=args.time_min,
            time_max=args.time_max,
            max_results=args.max_results,
            query=args.query,
            detailed=args.detailed,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_create_event(args):
    try:
        service = get_service("calendar")
        result = calendar.create_event(
            service,
            summary=args.summary,
            start_time=args.start,
            end_time=args.end,
            calendar_id=args.calendar_id,
            description=args.description,
            location=args.location,
            timezone=args.timezone,
            attendees=args.attendees,
            add_google_meet=args.add_meet,
            reminders=args.reminders,
            recurrence=args.recurrence,
            transparency=args.transparency,
            visibility=args.visibility,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_modify_event(args):
    try:
        service = get_service("calendar")
        result = calendar.modify_event(
            service,
            event_id=args.event_id,
            calendar_id=args.calendar_id,
            summary=args.summary,
            start_time=args.start,
            end_time=args.end,
            description=args.description,
            location=args.location,
            attendees=args.attendees,
            add_google_meet=args.add_meet,
            reminders=args.reminders,
            visibility=args.visibility,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_delete_event(args):
    try:
        service = get_service("calendar")
        result = calendar.delete_event(
            service,
            event_id=args.event_id,
            calendar_id=args.calendar_id,
        )
        success(result)
    except Exception as e:
        error(str(e))
