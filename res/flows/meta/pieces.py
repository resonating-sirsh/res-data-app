import res
from res.flows.meta.construction import clean as sew_clean
import stringcase
import pandas as pd

UPSERT_PIECE_COMPS = """mutation upsert_piece_components($piece_components: [meta_piece_components_insert_input!] = {}) {
  insert_meta_piece_components(objects: $piece_components, on_conflict: {constraint: piece_components_pkey, update_columns: [key, type, sew_symbol, commercial_acceptability_zone]}) {
    returning {
      id
      key
    }
  }
}
"""

UPSERT_PIECE_NAMES = """mutation upsert_piece_names($piece_names: [meta_piece_names_insert_input!] = {}) {
  insert_meta_piece_names(objects: $piece_names, on_conflict: {constraint: piece_names_pkey, update_columns: [key,name]}) {
    returning {
      key
      id
    }
  }
}
"""


def load_dictionary(reload=False):
    """
    use in favour of the cache. a simple piece component lookup on s3 as feather is fine for our purposes
    """
    airtable = res.connectors.load("airtable")
    s3 = res.connectors.load("s3")

    if not reload:
        return s3.read("s3://res-data-platform/cache/piece_components.feather")

    piece_components = airtable["appa7Sw0ML47cA8D1"]["tbllT2w5JpN8JxqkZ"].to_dataframe()
    pc = piece_components[
        [
            "Tag Abbreviation",
            "Tag Word",
            "Tag Category",
            "sew_symbols",
            "Commercial Acceptability Zone",
            "Last Modified",
        ]
    ]
    pc.columns = [stringcase.snakecase(c).replace("__", "_") for c in pc.columns]
    pc["sew_symbol_key"] = pc["sew_symbols"].map(
        lambda x: f"sew_grouping_symbols/{sew_clean(x)}" if pd.notnull(x) else None
    )
    s3.write("s3://res-data-platform/cache/piece_components.feather", pc)
    return pc


def get_piece_components_cache(just_use_airtable=False):
    """
    DEPRECATE
    bootstrap with
        # init
        # data = load_from_airtable()
        # data = data.drop_duplicates(subset=['key'])
        # c = CachedWarehouse(name, key="key", df=data)
        # c.write()

    """

    from res.connectors.snowflake import CachedWarehouse

    name = "piece_components"

    def load_from_airtable(missing=None):
        """
        todo the missing data resolver needs to efficiently load what is actually missing
        TODO
        """
        airtable = res.connectors.load("airtable")
        data = airtable["appa7Sw0ML47cA8D1"]["tbllT2w5JpN8JxqkZ"].to_dataframe()
        data = res.utils.dataframes.rename_and_whitelist(
            data,
            {
                "Name": "key",
                "Tag Abbreviation": "abbreviation",
                "Tag Word": "name",
                "Tag Category": "category",
            },
        )

        return data

    if just_use_airtable:
        return load_from_airtable()

    c = CachedWarehouse(name, key="key", missing_data_resolver=load_from_airtable)

    return c


VALID_PIECE_TYPES = ["BF", "X", "S", "LN", "C", "F"]
PRINTABLE_PIECE_TYPES = [
    "BF",
    "S",
    "LN",
    "C",
]


class PieceName:
    """
    Piece name parse and validation based on
    https://airtable.com/appa7Sw0ML47cA8D1/tbllT2w5JpN8JxqkZ/viwjOjQd6ETyRJdXV?blocks=hide

    Example piece
    pn =  PieceName('TK-6077-V7-DRSWTBLT-_MD')

    Displays SVG in Jupyter

    It is slightly concerning that the names are not fixed length
    Exploiting a mix of even and odd length parts, we can infer that if after removing what we think is a component
    there must be an even number of characters left because identity is 0, 2 or 4 in length and is at the end
    We could also do a context sensitive parse with lookups but that is costly because we need to load data and iterate


    """

    def __init__(
        self,
        name,
        validate=False,
        category_only=False,
        known_components=None,
        pad=False,
    ):
        if pad:
            name = f"XX-0000-V0-{name}"

        self._validation_errors = []

        try:
            name = name.split("+")[0]
            self._full = name
            self._key = name.split("_")[0].rstrip("-")

            idx = 3
            if category_only:
                idx = 0
            self._name = "-".join(self._key.split("-")[idx:])

            chk_comp = self._name[5:].split("-")[0]
            self._has_comp = len(chk_comp) >= 3 and len(chk_comp[3:]) % 2 == 0
            self._version = None
            self._version = int(self._key.split("-")[2].replace("V", ""))

        except Exception as ex:
            res.utils.logger.warn(f"Error loading piece {name} {ex}")
            self._validation_errors.append("PIECE_NAME_BAD")

        if validate:
            self.validate(known_components)

    def get_grouping_symbol(
        self, group_symbol_dictionary=None, light=False, color=None
    ):
        if group_symbol_dictionary is None:
            pc = load_dictionary()
            group_symbol_dictionary = dict(
                pc[["tag_abbreviation", "sew_symbol_key"]].dropna().values
            )

        symbol = self.get_grouping_symbol_name(
            group_symbol_dictionary=group_symbol_dictionary
        )
        if symbol:
            if color:
                symbol = res.utils.resources.read(f"{symbol}.svg", currentColor=color)
            elif light:
                symbol = res.utils.resources.read(
                    f"{symbol}.svg", color_rgba=(160, 160, 160, 255)
                )
            else:
                symbol = res.utils.resources.read(f"{symbol}.svg")

            return symbol

    def get_grouping_symbol_name(self, group_symbol_dictionary):
        s = group_symbol_dictionary
        symbol = s.get(self.component, s.get(self.part, s.get(self.product_taxonomy)))
        return symbol

    @staticmethod
    def get_piece_and_region_lookup_function():
        """
        Load the state of the pieces so we can determine a mapping between codes like BKYKE and their region
        """
        data = res.connectors.load("airtable").get_table_data(
            "appa7Sw0ML47cA8D1/tbllT2w5JpN8JxqkZ"
        )
        data = data[
            ["Name", "Tag Abbreviation", "Tag Word", "Tag Category", "sew_symbols"]
        ].to_dict("records")
        data = {r["Tag Abbreviation"]: r for r in data}

        def get_piece_and_region(code):
            if pd.isnull(code):
                return {}

            st = lambda x: "" if not x else x.lstrip().rstrip()

            try:
                part = code[:2]
                comp = code[2:]
                part = data.get(part)
                comp = data.get(comp)

                if part and comp:
                    # determine region
                    region = part["sew_symbols"]
                    if pd.isnull(region):
                        region = None
                    oregion = comp["sew_symbols"]

                    if not pd.isnull(oregion):
                        region = oregion

                    return {
                        "code": code,
                        "name": f"{st(part['Tag Word'])} {st(comp['Tag Word'])}",
                        "region": region,
                    }
            except:
                raise Exception(f"Failed for piece {code}")

            return {}

        return get_piece_and_region

    @property
    def name(self):
        return self._name

    @property
    def grouping_symbol(self):
        """
        because we reload the cache every time, this is more a convenience method for testing
        Better to call get_grouping_symbol after loading the cache for batch
        """
        pc = load_dictionary()
        lookup = dict(pc[["tag_abbreviation", "sew_symbol_key"]].dropna().values)
        return self.get_grouping_symbol(group_symbol_dictionary=lookup)

    @property
    def grouping_symbol_light(self):
        """
        like the grouping symbol but for now just have two modes - regular and lighter
        """
        pc = load_dictionary()
        lookup = dict(pc[["tag_abbreviation", "sew_symbol_key"]].dropna().values)
        return self.get_grouping_symbol(group_symbol_dictionary=lookup, light=True)

    @property
    def commercial_acceptability_zone(self):
        """ """
        pc = load_dictionary()
        lookup = dict(
            pc[["tag_abbreviation", "commercial_acceptability_zone"]].dropna().values
        )
        return lookup.get(self.part)

    def identity_prefix(self, prefix):
        key = self._key.split("-")[-2]

        return self._full.replace(key, f"{key}{prefix}")

    def validate(self, known_components=None, verbose=False, expected_version=None):
        """
        if we know the piece components that actually exist we can check exists in each part

        if we use known piece components we check if the pieces we have are actually containing components we know
        """
        if known_components is not None:
            self._validation_errors += self.check_components_exist(known_components)

        if expected_version is not None:
            if expected_version != self._version:
                res.utils.logger.debug(
                    f"Expected version {expected_version} does not match body piece version {self._version}"
                )
                self._validation_errors += ["PIECE_NAME_BAD_VERSION"]

        if self._validation_errors:
            if verbose:
                res.utils.logger.debug(f"The piece {self._name} has validation errors")
            return list(set(self._validation_errors))
        return []

    def check_components_exist(self, known_components):
        validation_errors = []

        r = known_components.query(
            f"category == 'Product Taxonomy Tag' & abbreviation == '{self.product_taxonomy}'"
        )
        if len(r) == 0:
            validation_errors.append("PIECE_NAME_UNKNOWN_PRODUCT_TAX")

        r = known_components.query(
            f"category == 'Components Tag' & abbreviation == '{self.component}'"
        )
        if len(r) == 0:
            validation_errors.append("PIECE_NAME_UNKNOWN_COMPONENT")

        r = known_components.query(
            f"category == 'Part Tag' & abbreviation == '{self.part}'"
        )
        if len(r) == 0:
            validation_errors.append("PIECE_NAME_UNKNOWN_PART")

        if self.identity:
            if len(self.identity) >= 2:
                r = known_components.query(
                    f"category == 'Orientation Tag' & abbreviation == '{self.identity[:2]}'"
                )
                if len(r) == 0:
                    validation_errors.append("PIECE_NAME_UNKNOWN_ORIENTATION")
            if len(self.identity) == 4:
                r = known_components.query(
                    f"category == 'Orientation Tag' & abbreviation == '{self.identity[2:]}'"
                )
                if len(r) == 0:
                    validation_errors.append("PIECE_NAME_UNKNOWN_ORIENTATION")

            # TODO could also check the left and top are in the right order but maybe does not matter

        r = list(
            known_components.query(f"category == 'Piece Type Tag'")["abbreviation"]
        )

        if self.piece_type not in r:
            validation_errors.append("PIECE_NAME_UNSUPPORTED_TYPE")

        return validation_errors

    @staticmethod
    def get_pairing_component(row):
        """
        This is a function of piece name but also has an extra geometry tag that can be added
        In this case of geometry tags, for example a piece like pocket that is applied onto another piece, the tag is used
        some components such as panels themselves can carry these component types
        """
        pc = PieceName(row["key"])

        has_pairing = row.get("has_pairing_qualifier")
        # there are a list of components that always take this
        # CONTRACTvpc.component in ["PNL", "PKT", "PLK"] and
        if has_pairing or pc.part in ["FT", "BK"]:
            return f"{pc.part}{pc.identity}"

    @property
    def product_taxonomy(self):
        return self._name[:3]

    @property
    def pose(self):
        return "left" if "LF" in self.orientation else "right"

    @property
    def part(self):
        return self._name[3:5]

    @property
    def component(self):
        """
        seems like the component is the only optional tag
        """
        return self._name[5:8] if self._has_comp == True else ""

    @property
    def orientation(self):
        idx = 8 if self._has_comp == True else 5
        return self._name[idx:].split("-")[0]

    @property
    def identity(self):
        return self.orientation

    @property
    def is_printable(self):
        return self.piece_type in PRINTABLE_PIECE_TYPES

    @property
    def is_cut_asset(self):
        return (
            self.piece_type in VALID_PIECE_TYPES
            and self.piece_type not in PRINTABLE_PIECE_TYPES
        )

    @property
    def piece_type(self):
        return self._name.split("-")[-1] if "-" in self._name else None

    @property
    def piece_type_name(self):
        t = self.piece_type
        if t == "S":
            return "Self"
        if t == "X":
            return "Stamper"
        if t == "F":
            return "Fused"
        if t == "BF":
            return "Block Fused"
        if t == "LN":
            return "Lining"
        if t == "C":
            return "Combo"

        return "unknown"

    # todo mack a nice SVG descriptor

    def __repr__(self):
        return self._key

    def _repr_svg_(self):
        prod = self.product_taxonomy
        part = self.part

        comp = self.component
        if comp == None or comp == "":
            comp = "-----"

        identity = self.orientation
        if len(identity) == 0:
            identity = "-------"
        if len(identity) == 2:
            identity = f"--{identity}--"

        return f"""<svg height="70" width="550">
            <g>
                <rect x="0" y="0" width="100" height="50" fill="white" stroke='black' stoke-width="1"></rect>
                <text x="10" y="40" font-family="Verdana" font-size="35" fill="black">{prod}</text>
                <text x="3" y="60" font-family="Verdana" font-size="10" fill="black">product taxonomy</text>
            </g>

             <g transform="translate(110,0)">
                <rect x="0" y="0" width="70" height="50" fill="white" stroke='black' stoke-width="1"></rect>
                <text x="8" y="40" font-family="Verdana" font-size="35" fill="black">{part}</text>
                <text x="25" y="60" font-family="Verdana" font-size="10" fill="black">part</text>
            </g>

            <g transform="translate(190,0)">
                <rect x="0" y="0" width="100" height="50" fill="white" stroke='black' stoke-width="1"></rect>
                <text x="10" y="40" font-family="Verdana" font-size="35" fill="black">{comp or '__'}</text>
                <text x="20" y="60" font-family="Verdana" font-size="10" fill="black">component</text>
            </g>

              <g transform="translate(300,0)">
                <rect x="0" y="0" width="120" height="50" fill="white" stroke='black' stoke-width="1"></rect>
                <text x="3" y="40" font-family="Verdana" font-size="35" fill="black">{identity}</text>
                <text x="40" y="60" font-family="Verdana" font-size="10" fill="black">identity</text>
            </g>

              <g transform="translate(430,0)">
                <rect x="0" y="0" width="70" height="50" fill="white" stroke='black' stoke-width="1"></rect>
                <text x="10" y="40" font-family="Verdana" font-size="35" fill="black">{self.piece_type or '---'}</text>
                <text x="23" y="60" font-family="Verdana" font-size="10" fill="black">type</text>
            </g>

        </svg>"""
