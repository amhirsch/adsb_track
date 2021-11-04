import hashlib
import struct


def session_hash(host: str, port: int, rawtype: str, start: float):
    """Creates a unique has for the session.

    Args:
        host (str): Session host
        port (int): Session port
        rawtype (str): Session rawtype
        start (float): Session start time.
    Returns:
        str: SHA-1 hash of the session.
    """
    h = hashlib.sha1()
    h.update(str.encode(host))
    h.update(int.to_bytes(port, 2, 'big'))
    h.update(str.encode(rawtype))
    h.update(bytes(struct.pack('d', start)))
    return h.hexdigest()