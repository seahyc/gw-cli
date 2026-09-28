"""Raw Google API passthrough using gw's own (Keychain-backed) credentials.

`gw api` used to always exec the external `gws` binary, which keeps its own
separate OAuth credential store. If a machine only ever ran `gw auth login`,
`gws` has no token of its own and any `gw api ...` call fails with
`invalid_grant`, even though every native `gw <service> ...` command works
fine using gw's Keychain-stored credentials.

This module builds a Discovery-based client with `gw.auth.get_credentials()`
(the same credentials every native command uses) and dynamically dispatches
`<resource>.<method>(**params)`, so any Google Workspace API surface works
without a second, separately-authenticated CLI.
"""

import json

from gw.auth import SERVICE_VERSIONS, get_credentials
from gw.output import success, error


def register(subparsers):
    parser = subparsers.add_parser(
        "api",
        help="Raw Google API passthrough: gw api <service> <resource> <method> [--params JSON] [--body JSON]",
    )
    parser.add_argument("api_service", help="Google API service name (drive, docs, sheets, gmail, calendar, forms, slides)")
    parser.add_argument("resource", help="Discovery resource path, dot-separated (e.g. 'files', 'comments.replies')")
    parser.add_argument("method", help="Method name on the resource (e.g. 'list', 'get', 'create')")
    parser.add_argument("--version", help="API version override (default: gw's standard version for the service)")
    parser.add_argument("--params", help="JSON object of keyword arguments for the method call")
    parser.add_argument("--body", help="JSON object to pass as the method's 'body' argument")
    parser.add_argument("--dry-run", action="store_true", help="Print the resolved call without executing it")
    parser.set_defaults(func=cmd_api)


def _resolve_resource(service, resource_path):
    node = service
    for part in resource_path.split("."):
        if not part:
            continue
        node = getattr(node, part)()
    return node


def cmd_api(args):
    try:
        from googleapiclient.discovery import build

        version = args.version or SERVICE_VERSIONS.get(args.api_service)
        if not version:
            error(
                f"Unknown service '{args.api_service}'. Pass --version explicitly, "
                f"or use one of: {', '.join(SERVICE_VERSIONS)}"
            )
            return

        params = json.loads(args.params) if args.params else {}
        if not isinstance(params, dict):
            error("--params must be a JSON object")
            return

        if args.body is not None:
            params["body"] = json.loads(args.body)

        if args.dry_run:
            success(json.dumps({
                "dry_run": True,
                "service": args.api_service,
                "version": version,
                "resource": args.resource,
                "method": args.method,
                "params": params,
            }, indent=2))
            return

        credentials, _ = get_credentials()
        service = build(args.api_service, version, credentials=credentials)
        resource = _resolve_resource(service, args.resource)
        method = getattr(resource, args.method)
        result = method(**params).execute()
        success(json.dumps(result, indent=2, default=str))
    except json.JSONDecodeError as e:
        error(f"Invalid JSON in --params/--body: {e}")
    except AttributeError as e:
        error(f"Unknown resource or method: {e}")
    except Exception as e:
        error(str(e))
