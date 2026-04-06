from redis_sync.sync_handler import parse_psync_response


def test_parse_psync_simple_string_fullresync():
    r = parse_psync_response(b"+FULLRESYNC abc123 42\r\n")
    assert r == ("FULLRESYNC", "abc123", 42)


def test_parse_psync_str_without_plus():
    r = parse_psync_response("FULLRESYNC rid 0")
    assert r == ("FULLRESYNC", "rid", 0)


def test_parse_psync_continue():
    r = parse_psync_response(b"+CONTINUE\r\n")
    assert r[0] == "CONTINUE"
    assert r[1] is None
    assert r[2] is None


def test_parse_psync_list_form():
    r = parse_psync_response([b"FULLRESYNC", b"x", b"1"])
    assert r == ("FULLRESYNC", "x", 1)
