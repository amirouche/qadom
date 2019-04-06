import asyncio
import logging
import os

import aiohttp_jinja2
import uvloop
import daiquiri
import jinja2
from aiohttp import web
from itsdangerous import BadSignature
from itsdangerous import SignatureExpired
from itsdangerous import TimestampSigner
from aiohttp import ClientSession
from aiohttp.web import normalize_path_middleware

from setproctitle import setproctitle  # pylint: disable=no-name-in-module

import hoply as h
from hoply.leveldb import LevelDBConnexion

from qadom import VERSION
from qadom import HOMEPAGE
from qadom.peer import make_peer


log = logging.getLogger(__name__)
daiquiri.setup(logging.INFO, outputs=('stderr',))


def no_auth(handler):
    """Decorator to tell the ``middleware_check_auth`` to not check for the token

    """
    handler.no_auth = True
    return handler


async def middleware_check_auth(app, handler):
    """Check that the request has a valid token.

    Raise HTTPForbidden when the token is not valid.

    `handler` can be marked to ignore token. This is useful for pages like
    account login, account creation and password retrieval

    """

    async def middleware_handler(request):
        if request.path.startswith('/static/') or getattr(handler, "no_auth", False):
            response = await handler(request)
            return response
        else:
            # then the route requires authentication
            try:
                token = request.cookies['token']
            except KeyError:
                log.debug("No auth token found")
                raise web.HTTPFound(location="/")
            else:
                max_age = app["settings"].TOKEN_MAX_AGE
                try:
                    app["signer"].unsign(token, max_age=max_age)
                except SignatureExpired:
                    log.debug("Token expired")
                    # TODO: redirect to login page with a next parameter
                    raise web.HTTPFound(location="/login")
                except BadSignature:
                    log.debug("Bad signature")
                    raise web.HTTPFound(location="/")
                else:
                    request.logged = True
                    response = await handler(request)
                    return response

    return middleware_handler


@no_auth
async def status(request):
    """Check that the app is properly working"""

    return request.app.render('index.html', request, {})

# boot the app


async def init(app):
    log.debug("init database")
    cnx = LevelDBConnexion('/tmp/qadom/')
    hoply = h.open(cnx, 'qadom', ('collection', 'identifier', 'key', 'value'))
    app["hoply"] = hoply

    log.debug("init peer")
    # TODO: retrieve port from config
    # TODO: fetch uid from database
    # TODO: fetch private_key from database
    peer = await make_peer(42, 9999, hoply=hoply)
    app["peer"] = peer
    return app


def create_app(loop):
    """Starts the aiohttp process to serve the REST API"""
    # TODO: FIXME
    from qadom import settings

    # setup logging
    level_name = os.environ.get("DEBUG", "INFO")
    level = getattr(logging, level_name)
    daiquiri.setup(level=level, outputs=("stderr",))

    setproctitle("socialiter")

    log.info("init qadom %s", VERSION)

    # init app
    app = web.Application()  # pylint: disable=invalid-name
    aiohttp_jinja2.setup(app, loader=jinja2.FileSystemLoader('qadom/templates'))
    app.on_startup.append(init)
    app.middlewares.append(normalize_path_middleware())
    app.middlewares.append(middleware_check_auth)
    #
    app["settings"] = settings
    app["signer"] = TimestampSigner(settings.SECRET)
    user_agent = "socialiter {} ({})".format(VERSION, HOMEPAGE)
    headers = {"User-Agent": user_agent}
    app["session"] = ClientSession(headers=headers)
    # handy shortcut
    app.render = aiohttp_jinja2.render_template
    # hop this helps
    app.router.add_route("GET", "/status/", status)
    app.router.add_static('/static/', path='qadom/static')
    return app


def main():
    """entry point of the whole application, equivalent to django's manage.py"""
    setproctitle("socialiter")

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.get_event_loop()
    app = create_app(loop)
    log.info("running webserver on http://0.0.0.0:8000")
    web.run_app(app, host="0.0.0.0", port=8000)
