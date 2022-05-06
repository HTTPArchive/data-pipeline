import hashlib
import logging
import os

import dateutil.parser

from modules import constants


def remove_empty_keys(d):
    return {k: v for k, v in d if v is not None}


def get_url_hash(url):
    return int(hashlib.md5(url.encode()).hexdigest()[0:4], 16)


def get_ext(ext):
    ret_ext = ext
    i_q = ret_ext.find("?")
    if i_q > -1:
        ret_ext = ret_ext[:i_q]

    ret_ext = ret_ext[ret_ext.rfind("/") + 1:]
    i_dot = ret_ext.rfind(".")
    if i_dot == -1:
        ret_ext = ""
    else:
        ret_ext = ret_ext[i_dot + 1:]
        if len(ret_ext) > 5:
            # This technique can find VERY long strings that are not file extensions. Try to weed those out.
            ret_ext = ""

    return ret_ext


def pretty_type(mime_typ, ext):
    mime_typ = mime_typ.lower()

    # Order by most unique first.
    # Do NOT do html because "text/html" is often misused for other types. We catch it below.
    for typ in ["font", "css", "image", "script", "video", "audio", "xml"]:
        if typ in mime_typ:
            return typ

    # Special cases I found by manually searching.
    if "json" in mime_typ or ext in ["js", "json"]:
        return "script"
    elif ext in ["eot", "ttf", "woff", "woff2", "otf"]:
        return "font"
    elif ext in [
        "png",
        "gif",
        "jpg",
        "jpeg",
        "webp",
        "ico",
        "svg",
        "avif",
        "jxl",
        "heic",
        "heif",
    ]:
        return "image"
    elif ext == "css":
        return "css"
    elif ext == "xml":
        return "xml"
    # Video extensions mp4, webm, ts, m4v, m4s, m4v, mov, ogv
    elif next(
        (typ for typ in ["flash", "webm", "mp4", "flv"] if typ in mime_typ), None
    ) or ext in ["mp4", "webm", "ts", "m4v", "m4s", "mov", "ogv", "swf", "f4v", "flv"]:
        return "video"
    elif "html" in mime_typ or ext in ["html", "htm"]:
        # Here is where we catch "text/html" mime type.
        return "html"
    elif "text" in mime_typ:
        # Put "text" LAST because it's often misused so $ext should take precedence.
        return "text"
    else:
        return "other"


def get_format(pretty_typ, mime_typ, ext):
    if "image" == pretty_typ:
        # Order by most popular first.
        for typ in [
            "jpg",
            "png",
            "gif",
            "webp",
            "svg",
            "ico",
            "avif",
            "jxl",
            "heic",
            "heif",
        ]:
            if typ in mime_typ or typ == ext:
                return typ
        if "jpeg" in mime_typ:
            return "jpg"
    if "video" == pretty_typ:
        # Order by most popular first.
        for typ in ["flash", "swf", "mp4", "flv", "f4v"]:
            if typ in mime_typ or typ == ext:
                return typ
    return ""


# Headers can appear multiple times, so we have to concat them all then add them to avoid setting a column twice.
def parse_header(input_headers, standard_headers, cookie_key, output_headers=None):
    if output_headers is None:
        output_headers = {}
    other = []
    cookie_size = 0
    for header in input_headers:
        name = header["name"]
        lc_name = name.lower()
        value = header["value"][:255]
        orig_value = header["value"]
        if lc_name in standard_headers.keys():
            # This is one of the standard headers we want to save
            column = standard_headers[lc_name]
            if output_headers.get(column):
                output_headers[column].append(value)
            else:
                output_headers[column] = [value]
        elif cookie_key == lc_name:
            # We don't save the Cookie header, just the size.
            cookie_size += len(orig_value)
        else:
            # All other headers are lumped together.
            other.append("{} = {}".format(name, orig_value))

    # output_headers = {k: ", ".join(v) for k, v in output_headers.items()}
    ret_other = ", ".join(other)

    return output_headers, ret_other, cookie_size


def client_name(file_name):
    dir_name, base_name = os.path.split(file_name)
    client = dir_name.split("/")[-1].split("-")[0]

    if client == "chrome" or "_Dx" in base_name:
        return "desktop"
    elif client == "android" or "_Mx" in base_name:
        return "mobile"
    else:
        return client.lower()


def format_table_name(row, table):
    return "{}.{}_{}".format(
        constants.bigquery["datasets"][table], row["date"], row["client"]
    )


def datetime_to_epoch(dt, status_info):
    try:
        return int(round(dateutil.parser.parse(dt).timestamp()))
    except dateutil.parser.ParserError:
        logging.warning(
            f"Could not parse datetime to epoch. dt={dt},status_info={status_info}"
        )
        return None


def crawl_date(dir_name):
    return dateutil.parser.parse(
        dir_name.split("/")[-1].split("-")[1].replace("_", " ")
    )