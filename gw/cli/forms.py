"""CLI commands for Google Forms."""

from gw.auth import get_service
from gw.output import success, error
from gw.services import forms


def register(subparsers):
    parser = subparsers.add_parser("forms", help="Google Forms operations")
    sub = parser.add_subparsers(dest="action", required=True)

    # forms create
    p_create = sub.add_parser("create", help="Create a new form")
    p_create.add_argument("--title", required=True)
    p_create.add_argument("--description")
    p_create.add_argument("--document-title")

    # forms read
    p_read = sub.add_parser("read", help="Read a form's details")
    p_read.add_argument("form_id")

    # forms responses
    p_responses = sub.add_parser("responses", help="List form responses")
    p_responses.add_argument("form_id")
    p_responses.add_argument("--max-results", type=int, default=10)
    p_responses.add_argument("--page-token")

    # forms response
    p_response = sub.add_parser("response", help="Get a single form response")
    p_response.add_argument("form_id")
    p_response.add_argument("response_id")

    # forms publish-settings
    p_publish = sub.add_parser("publish-settings", help="Update publish settings")
    p_publish.add_argument("form_id")
    p_publish.add_argument("--template", type=lambda v: v.lower() in ("true", "1", "yes"), default=False, help="Publish as template")
    p_publish.add_argument("--require-auth", type=lambda v: v.lower() in ("true", "1", "yes"), default=False, help="Require authentication")


def handle(args):
    action = args.action
    if action == "create":
        cmd_create(args)
    elif action == "read":
        cmd_read(args)
    elif action == "responses":
        cmd_responses(args)
    elif action == "response":
        cmd_response(args)
    elif action == "publish-settings":
        cmd_publish_settings(args)


def cmd_create(args):
    try:
        service = get_service("forms")
        result = forms.create_form(
            service,
            title=args.title,
            description=args.description,
            document_title=args.document_title,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_read(args):
    try:
        service = get_service("forms")
        result = forms.get_form(service, form_id=args.form_id)
        success(result)
    except Exception as e:
        error(str(e))


def cmd_responses(args):
    try:
        service = get_service("forms")
        result = forms.list_form_responses(
            service,
            form_id=args.form_id,
            page_size=args.max_results,
            page_token=args.page_token,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_response(args):
    try:
        service = get_service("forms")
        result = forms.get_form_response(
            service,
            form_id=args.form_id,
            response_id=args.response_id,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_publish_settings(args):
    try:
        service = get_service("forms")
        result = forms.set_publish_settings(
            service,
            form_id=args.form_id,
            publish_as_template=args.template,
            require_authentication=args.require_auth,
        )
        success(result)
    except Exception as e:
        error(str(e))
