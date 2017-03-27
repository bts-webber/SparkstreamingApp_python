import urlparse
def get_payload(url):
    parser=urlparse.urlparse(url)
    payload=parser.params+parser.query+parser.fragment
    return payload
def get_path(url):
    parser=urlparse.urlparse(url)
    return parser.path