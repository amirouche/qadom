import asyncio
import logging
import daiquiri
from qadom.peer import make_peer
from qadom.peer import make_uid


daiquiri.setup(logging.DEBUG, outputs=('stderr',))


loop = asyncio.get_event_loop()
peer = loop.run_until_complete(make_peer(make_uid(), 9999))
