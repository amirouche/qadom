import asyncio
import logging
import os
from time import time

import aiohttp_jinja2
import msgpack
import uvloop
import daiquiri
import jinja2
from aiohttp import web
from itsdangerous import BadSignature
from itsdangerous import SignatureExpired
from itsdangerous import TimestampSigner
from aiohttp import ClientSession
from aiohttp.web import normalize_path_middleware
from humanize import naturaldelta
from setproctitle import setproctitle  # pylint: disable=no-name-in-module

import hoply as h
from hoply.leveldb import LevelDBConnexion

from qadom import VERSION
from qadom import HOMEPAGE
from qadom.peer import make_peer
from qadom.peer import make_uid
from qadom.peer import hash


log = logging.getLogger(__name__)
daiquiri.setup(logging.INFO, outputs=("stderr",))


def pick(*names):
    # TODO: move to hoply project
    def wrapped(iterator):
        for bindings in iterator:
            yield tuple(bindings.get(name) for name in names)

    return wrapped


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
        request.logged = False
        if request.path.startswith("/static/"):
            response = await handler(request)
            return response
        else:
            # the route MIGHT require authentication
            try:
                token = request.cookies["token"]
            except KeyError:
                if getattr(handler, "no_auth", False):
                    response = await handler(request)
                    return response
                else:
                    log.debug("No auth token found")
                    raise web.HTTPFound(location="/")
            else:
                max_age = app["settings"].TOKEN_MAX_AGE
                try:
                    app["signer"].unsign(token, max_age=max_age)
                except SignatureExpired:
                    log.debug("Token expired")
                    raise web.HTTPFound(location="/login")
                except BadSignature:
                    if getattr(handler, "no_auth", False):
                        out = await handler(request)
                        return out
                    else:
                        log.warning("Bad signature")
                        raise web.HTTPFound(location="/")
                else:
                    request.logged = True
                    response = await handler(request)
                    return response

    return middleware_handler


@no_auth
async def index(request):
    @h.transactional
    def query(tr):
        out = h.compose(
            tr.FROM("QADOM:QUESTION", h.var("key"), "question", h.var("question")),
            tr.where("QADOM:QUESTION", h.var("key"), "created-at", h.var("created-at")),
            tr.where("QADOM:QUESTION", h.var("key"), "author", h.var("author")),
        )
        out = list(out)
        # fetch tags too
        questions = []
        for bindings in out:
            tags = [
                x["tag"]
                for x in tr.FROM("QADOM:QUESTION", bindings["key"], "tag", h.var("tag"))
            ]  # noqa
            question = bindings.set("tags", tags)
            questions.append(question)
        return questions

    hoply = request.app["hoply"]
    questions = await request.app.run(query, hoply)
    await request.app.run(debug, hoply)

    now = int(time())
    context = dict(
        logged=request.logged, questions=questions, naturaldelta=naturaldelta, now=now
    )
    return request.app.render("index.html", request, context)


@no_auth
async def ask(request):
    if request.method == "GET":
        return request.app.render("ask.html", request, dict(logged=request.logged))
    else:
        data = await request.post()
        question = data["question"]
        tags = [x.strip() for x in data["tags"].split(",") if x.strip()]

        # publish on the network
        peer = request.app["peer"]
        key = await peer.set(msgpack.packb((question, tags, None)))
        await peer.bag(hash(b"qadom"), key)

        # save locally
        @h.transactional
        def save(tr, question, tags, key):
            now = int(time())
            tr.add("QADOM:QUESTION", key, "question", question)
            for tag in tags:
                tr.add("QADOM:QUESTION", key, "tag", tag)
            tr.add("QADOM:QUESTION", key, "created-at", now)
            tr.add("QADOM:QUESTION", key, "author", None)

        hoply = request.app["hoply"]
        await request.app.run(save, hoply, question, tags, key)

        raise web.HTTPFound("/ack/")


@no_auth
async def ack(request):
    return request.app.render("ack.html", request, dict(logged=request.logged))


@no_auth
async def status(request):
    """Check that the app is properly working"""
    return request.app.render("status.html", request, dict())


async def fetch(app, key):
    peer = app["peer"]

    try:
        document = await peer.get(key)
    except KeyError:
        log.warning("not found key=%r", key)
        return
    document = msgpack.unpackb(document, raw=False)
    question, tags, author = document

    log.critical(document)

    @h.transactional
    def maybe_save(tr):
        if not tr.ask("QADOM:QUESTION", key, "question", question):
            now = int(time())
            tr.add("QADOM:QUESTION", key, "question", question)
            for tag in tags:
                tr.add("QADOM:QUESTION", key, "tag", tag)
            tr.add("QADOM:QUESTION", key, "created-at", now)
            tr.add("QADOM:QUESTION", key, "author", None)

    hoply = app["hoply"]
    await app.run(maybe_save, hoply)


async def refresh(app):
    peer = app["peer"]
    while True:
        keys = await peer.bag(hash(b"qadom"))
        for key in keys:
            asyncio.ensure_future(fetch(app, key))
        # let's go to sleep
        await asyncio.sleep(10)


# boot the app


async def init(app):
    port = int(os.environ.get("QADOM_PORT", 8001))

    log.debug("init database")
    cnx = LevelDBConnexion("/tmp/qadom" + str(port))
    hoply = h.open(cnx, "qadom", ("collection", "identifier", "key", "value"))
    app["hoply"] = hoply

    log.debug("init peer")

    peer = await make_peer(make_uid(), port, hoply=hoply)
    bootstrap = os.environ.get("BOOTSTRAP")
    if bootstrap is not None:
        bootstrap = ("127.0.0.1", int(bootstrap))
        await peer.bootstrap(bootstrap)

    asyncio.ensure_future(refresh(app))

    # share the executor, TODO: pass executor as run to make_peer and
    # make it configurable
    app.run = peer._run
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
    aiohttp_jinja2.setup(app, loader=jinja2.FileSystemLoader("qadom/templates"))
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
    # hope this helps
    app.router.add_route("GET", "/status/", status)
    app.router.add_static("/static/", path="qadom/static")
    # routes
    app.router.add_route("GET", "/", index)
    app.router.add_route("GET", "/ask/", ask)
    app.router.add_route("POST", "/ask/", ask)
    app.router.add_route("GET", "/ack/", ack)
    return app


def main():
    """entry point of the whole application, equivalent to django's manage.py"""
    setproctitle("socialiter")
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.get_event_loop()
    app = create_app(loop)
    log.info("running webserver on http://0.0.0.0:8000")
    web.run_app(app, host="0.0.0.0", port=8000)
