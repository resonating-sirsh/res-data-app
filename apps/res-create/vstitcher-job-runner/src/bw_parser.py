import sys
import os
import sqlite3
import zstd
import json


def to_lines(blob):
    is_printable = lambda x: x >= 32 and x <= 126
    was_char = False
    line = ""
    lines = []
    for i in range(0, len(blob)):
        if is_printable(blob[i]):
            if not was_char:
                lines.append(line)
                line = ""
            line += chr(blob[i])
            was_char = True
        else:
            if was_char:
                lines.append(line)
                line = ""
            line += f"{blob[i]:02x} "
            was_char = False
        # if len(lines) > 0 and lines[-1].startswith("shapeId"):
        #     print("hi")

    return lines


def parse_rulers_from_blob(blob):
    i = 0

    def look_ahead(blob, i, target):
        t = 0
        length = len(target)
        while t < length and (i + t) < len(blob):
            if blob[i + t] == target[t]:
                t += 1
            else:
                return -1

            if t == len(target):
                return i + t
        return -1

    # keep reading until we come across "Ruler"
    # then look for a set of shapeId & edgeId/internalLineId
    # finishing off with "rulerName"
    in_ruler = False
    getting_shapeId = False
    getting_edgeId = False
    getting_internalLineId = False

    shapeId = None
    edgeId = None
    internalLineId = None
    components = []
    rulers_by_name = {}

    while i < len(blob):
        if not in_ruler:
            r = look_ahead(blob, i, b"Ruler")
            if r != -1:
                in_ruler = True
                getting_shapeId = True
                i = r
        else:
            n = look_ahead(blob, i, b"rulerName")
            if n != -1:
                # there are two bytes \x00 and \x04, then the next byte is the length of the rulerName
                ruler_name_length = blob[n + 2]
                ruler_name = blob[n + 3 : n + 3 + ruler_name_length].decode("utf-8")

                # all done, save these shapeId/<line>Id pairs
                in_ruler = False
                rulers_by_name[ruler_name] = components
                components = []
                i = n + 3 + ruler_name_length
                continue

            s = look_ahead(blob, i, b"shapeIds")
            if getting_shapeId and s != -1:
                # after shapeIds, there's some binary stuff we don't care about
                # "\x00\x00\x03Bin\x00\x0e\x05\x01" so let's skip this
                i = s + 10

                # the next 4 bytes are the shapeId in little-endian order
                shapeId = int.from_bytes(blob[i : i + 4], "little")

                getting_shapeId = False
                getting_edgeId = True
                i += 4
                continue

            e = look_ahead(blob, i, b"edgeId")
            if getting_edgeId and e != -1:
                # there are 2 chars after this we ignore "\x00\x03"
                i = e + 2
                edgeId = int.from_bytes(blob[i : i + 4], "little", signed=True)
                if edgeId == -1:
                    getting_internalLineId = True
                    getting_edgeId = False
                else:
                    components.append({"shapeId": shapeId, "edgeId": edgeId})
                    getting_edgeId = False
                    getting_shapeId = True
                i += 4
                continue

            l = look_ahead(blob, i, b"internalLineId")
            if getting_internalLineId and l != -1:
                # there are 2 chars after this we ignore "\x00\x03"
                i = l + 2
                internalLineId = int.from_bytes(blob[i : i + 4], "little", signed=True)
                if internalLineId != -1:
                    components.append(
                        {"shapeId": shapeId, "internalLineId": internalLineId}
                    )
                else:
                    pass  # unexpected, will skip to next ruler anyways
                getting_internalLineId = False
                getting_shapeId = True
                i += 4
                continue
        i += 1
    return rulers_by_name


# def parse_rulers(lines):
#     # keep reading until we come across "Ruler"
#     # then look for a set of shapeId & edgeId/internalLineId
#     # finishing off with "rulerName"
#     in_ruler = False
#     getting_shapeId = False
#     getting_edgeId = False
#     getting_internalLineId = False

#     shapeId = None
#     edgeId = None
#     internalLineId = None
#     components = []
#     rulers_by_name = {}

#     def hex_tokens_to_int(tokens):
#         # need to reverse the tokens, because they're in little-endian order
#         return int("".join(tokens[::-1]), 16)

#     i = 0
#     while i < len(lines):
#         line = lines[i]
#         if line.startswith("Ruler"):
#             in_ruler = True
#             getting_shapeId = True
#             i += 1
#             continue
#         if in_ruler:
#             if line.startswith("rulerName"):
#                 # skip forward 2 lines to get the name
#                 i += 2
#                 ruler_name = lines[i]

#                 # if the start isn't a letter, chomp it
#                 if not ruler_name[0].isalpha():
#                     ruler_name = ruler_name[1:]

#                 # all done, save these shapeId/<line>Id pairs
#                 in_ruler = False
#                 rulers_by_name[ruler_name] = components
#                 components = []
#                 i += 1
#                 continue

#             # look for shapeIds
#             if getting_shapeId and line.startswith("shapeIds"):
#                 # after shapeIds, there's some binary stuff we don't care about
#                 # then "Bin", which we also don't care about
#                 i += 3
#                 # this line might look like:
#                 # 00 0e 05 01 04 00 00 00 00 00 06
#                 #             ^^                    get this token
#                 # or it might look like this with a new line:
#                 # 00 0e 05 01
#                 # .                                 get this ascii and convert
#                 #
#                 line = lines[i]
#                 tokens = line.split(" ")
#                 if len(tokens) > 5:
#                     shapeId = int(tokens[4], 16)
#                 else:  # next line
#                     ascii = lines[i + 1]
#                     shapeId = ord(ascii)

#                 getting_shapeId = False
#                 getting_edgeId = True
#                 i += 1
#                 continue
#             if getting_edgeId and line.startswith("edgeId"):
#                 # this is valid if it's not ff ff ff ff (-1 in VStitcher)
#                 i += 1
#                 edge_bin = lines[i]
#                 tokens = edge_bin.split(" ")
#                 if "".join(tokens[2:6]) != "ffffffff":
#                     # got an edge id, save and move on to the next shapeId/<line>Id pair
#                     edgeId = hex_tokens_to_int(tokens[2:6])
#                     getting_edgeId = False
#                     getting_shapeId = True
#                     components.append(
#                         {
#                             "shapeId": shapeId,
#                             "edgeId": edgeId,
#                         }
#                     )
#                 else:
#                     # no edge id, so try internalLineId next
#                     getting_edgeId = False
#                     getting_internalLineId = True
#                 i += 1
#                 continue
#             if getting_internalLineId and line.startswith("internalLineId"):
#                 # this is valid if it's not ff ff ff ff (-1 in VStitcher)
#                 i += 1
#                 internalLine_bin = lines[i]
#                 tokens = internalLine_bin.split(" ")
#                 if "".join(tokens[2:6]) != "ffffffff":
#                     # got an internalLineId, save and move on to the next shapeId/<line>Id pair
#                     internalLineId = hex_tokens_to_int(tokens[2:6])
#                     getting_internalLineId = False
#                     getting_shapeId = True
#                     components.append(
#                         {
#                             "shapeId": shapeId,
#                             "internalLineId": internalLineId,
#                         }
#                     )
#                 else:
#                     # shouldn't get here, if there's no edgeId or internalLineId, something's wrong
#                     # skip for now
#                     print("Odd....")
#                     getting_internalLineId = False
#                     getting_shapeId = True
#                     i += 1
#                 continue
#         i += 1
#     return rulers_by_name


def main():
    if len(sys.argv) != 2:
        print("Usage: python3 bw_parser.py <bw_file>")
        sys.exit(1)

    # get the first arg as the .bw file, check it exists
    bw_file = sys.argv[1]
    # bw_file = "/Users/john/vstitcher_exports/job_runner_output/0bcb4401-3c9a-43c2-adad-26fb21cb39f0/TK-6129-V10-3D_BODY.bw"
    if not os.path.isfile(bw_file):
        print("Error: file does not exist")
        sys.exit(1)

    parent_dir = os.path.dirname(bw_file)
    output_json = os.path.join(parent_dir, "bw_parsed.json")

    # open the file and read the contents
    con = sqlite3.connect(bw_file)
    cur = con.cursor()

    garment_blob = cur.execute(
        "SELECT content FROM file_tree WHERE ext='.vsgx';"
    ).fetchone()[0]

    # decompress the garment blob
    garment_uncompressed = zstd.ZSTD_uncompress(garment_blob)

    result = {
        # convert it into lines separated by when it changes from ascii to hex
        # "rulers": parse_rulers(to_lines(garment_uncompressed)),
        # I *think* this is more reliable, does it byte by byte
        "rulers": parse_rulers_from_blob(garment_uncompressed),
    }

    # write the result to a json file
    with open(output_json, "w") as f:
        json.dump(result, f, indent=4)


if __name__ == "__main__":
    main()
