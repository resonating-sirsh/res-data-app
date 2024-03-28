"""

strategy here is to combine a specific DXF parser and a tech pack parser
the tech pack parser needs to extract text and image scans from pdf including sub images in pdf
each image is observed; assume sketches and tabular data
generally we are always extracting piece info and seams and POMs as well as top level body
we really need all te pieces
things like trims and buttons also need to be extracted ultimately but the PDF can always be used as reference 

notes:
we use height comparisons on rect pieces (rect pieces requires special attention generally) - in this case we can match the long CF placket with the front panel
button piece notes was useful to make sure we get the short hand details on the number of buttons / notes are generally useful for attention
i want to get sketch extract right on piece distinctness

Note i tried responding in a json format because i noticed some new line formatting text which may have misrepresented tabular data when parsed
For sketch descriptions etc that is not so important
the yaml format on pdf parsing seems useful to make sure we dont incorrectly delimit some facts

i have disabled the inclusion of the pdf-scan as the text parsing seems to extract they key results augmented with the image inspection
we do save the scan observations but we dont include it by default in the deep summary of the page (we should look into smartly assimilating them later in case they add value)

it understands well the idea of re-classification. for example moving "pleats" from pieces to seam operations makes sense. find a home for things that are causing problems

Multiple choices - requires more sewing intel
1. are dog house plackets always made with two pieces - that could be the clue we need (always look for evidence that it is a fact that we need the under )


rules:
- for panels, is the left panel the wider one with the placket?
- the collars seem to have buttons on one side
- are collars and sleeve cuffs always used (top and bottom) or just depends on notes
- how do the PoMs relate to pattern piece size and seam allowance
- the cuff pattern pieces are probably not left and right if there are two but top and under fabrics... we need to know that on our side but the symmetry exists in terms of the number of pieces

"""

import PyPDF2
import fitz
from PIL import Image, ImageOps
import io
from pdf2image import convert_from_path
import typing
from pydantic import BaseModel
from typing import List
import json
from res.media.images.providers.dxf import DxfFile

from res.learn.agents.BodyOnboarder import BodyOnboarder, PieceRefiner
from res.learn.agents.builder.utils import ask, describe_visual_image
import res
import numpy as np
from res.observability.io import ColumnarDataStore
from tenacity import retry, wait_fixed, stop_after_attempt

# from res.learn.models import BodyPiecesModel
from res.learn.optimization.nest import nest_dataframe
from res.flows.meta.ONE.geometry_ex import geometry_df_to_svg
from res.media.images.geometry import (
    is_polygon_rectangular,
    has_x_symmetry,
    has_y_symmetry,
    shift_geometry_to_origin,
    scale_shape_by,
    invert_axis,
    unary_union,
    LinearRing,
    Polygon,
)
from cairosvg import svg2png
import io

from res.observability.types import ResonanceSizesModel, PieceComponentsModel

try:
    from res.learn.models.BodyPiecesModel import BodyPiecesModel
except:
    res.utils.logger.info(f"Cannot include the DL model - BodyPiecesModel")

IMAGE_FORMAT = "jpeg"  # png

CONSTRUCTION_WISDOM = """
**construction notes**

1. bodice is broken into front and back panels and panels may be split into left and right parts depending on the design - this area may have large, long plackets or belts or other binding pieces
2. sleeve area on both the left and the right are broken down into a main sleeve panel and may have cuffs, plackets and tabs - plackets can be broken into a TOP and an UNDER part for structure
3. neck areas are broken down into collars and collar stands and may have small binding pieces
4. on some garments there may be defined bottom areas e.g pants or skirts
5. all garments can possibly have pockets on various panels
6. certain pieces can be fused for structure based on garment manufacturing best practices or special request from the designer
"""

PATTERN_MAKING = """Picturing the progress of making patterns,
1) They will start from 'sloper';  most of time it is the panel based pieces that will carry/ receive more "small" pieces depending on the design.
- sloper usually has front and back, and sleeve ( by region, in terms of body coverage )
- sloper helps creates the panels of front and back, set the foundation of fit.
2) add details/ adding pieces
- If needs to divide panels depends on two request:
   - if there is fit requirement that one piece cannot deliver. 
   - if there is opening of the entire body.   if there is,  there must be left and right,   and if it is symmetrical depends on how the edge meeds to be finished.
   - divide to create seam for aesthetics
- if has "add-on" pieces to
   - finish the edge.   eg. binding, cuff, collar, waistband
   - deliver a function.   like.  pocket,  placket,
   - aesthetics.
Another way to look at add-on pieces is by how it is added.
- interrupt the seam, partially
- interrupt the seam, entirely.
- interrupt the surface, partially, 
- interrupt the surface, entirely i.e. "layer up"  so the piece is either the same shape of the base / the panel / the receiver
- does not attach to any piece i.e. independent add-on
"""


class StaticMetaOneDxfFile(DxfFile):
    """
    sub class
    """

    @retry(wait=wait_fixed(5), stop=stop_after_attempt(2), reraise=True)
    def describe_image(self, image: Image, prompt: str, uri: str):
        """
        give a numpy or PIL image or a uri to an image - describe it

        by writing the image to s3 we can ask the agent to describe it

        name: is optional for a handle to the image otherwise everyone writes to the same place which is fine for testing
        prompt: what are we trying to extract
        """

        s3 = res.connectors.load("s3")
        s3.write(uri, image)
        if not s3.exists(uri):
            raise Exception(f"That file {uri} was not saved")
        res.utils.logger.debug(f"describing {uri} - {prompt}")
        desc = describe_visual_image(
            s3.generate_presigned_url(uri), question_relating_to_url_passed=prompt
        )

        res.utils.logger.debug(desc)
        return {"uri": uri, "description": desc}

    def add_part_explanations(self, data):
        """
        this would come from a knowledge store in reality - playing with the art of the possible in prompting the beast
        """

        def explain_part(row):
            """"""
            part = row["piece_code"]
            rect = row["is_rectangular"]

            if rect:
                return f"""Rectangular pieces are usually bindings, bias, plackets, tabs, ruffles etc. More intricately designed panels are usually are not rectangular.
                REctangular pieces cannot be classified on shape alone but on their relative size and relation to know pieces. 
                Observe the relative size rank. 
                If a piece is very big or small, check what pieces its most similar in size to which could have a clue about the function. For example, a rectangular piece that is the same size as as another panel might be related to that panel as a placket or binding"""

            if part == "BKYKE":
                return f"""Yoke pieces should be symmetric. If they are not we can mirror the pattern pieces for Left (LF) and right (RT) to complete the piece. If the piece is double or has facing we can then duplicate the pieces to provide top (TP) and under (UN) pieces from the pattern"""

            if "NK" in part[:2]:
                return f"""Neck pieces usually come in pairs in the pattern. For example Collar and Collar stand pieces often have both Top and Under pieces provided """

            if part in ["BKPNL", "FTPNL"]:
                return "Bodice panel pieces should be symmetric (typically) to complete the bodice. If there is only one piece then it likely has an X symmetry. If there are two front or back panels they are typically for the Right (RT) and Left (LF)"

            if "SL" in part[:2]:
                s = """Sleeve pieces are typically mirrored left and right."""
                if "CUF" in part:
                    s += f" Cuff pieces are often made of two fabric pieces. If the pattern has two cuff pieces its likely a top and under piece rather than a left and right piece unless all the sleeve pieces are duplicated"
                return s

        data["relative_piece_area_rank"] = data["piece_area_inches"].rank()
        data["relative_piece_height_rank"] = data["piece_height_inches"].rank()
        data["relative_rectangular_piece_height_rank"] = None
        data.loc[
            data["is_rectangular"], "relative_rectangular_piece_height_rank"
        ] = data["relative_piece_height_rank"]
        data["relative_rectangular_piece_height_rank"] = data[
            "relative_rectangular_piece_height_rank"
        ].rank()
        data = res.utils.dataframes.replace_nan_with_none(data)
        data["notes"] = data.apply(explain_part, axis=1)
        return data

    def parse_for_meta_one(
        self,
        body_key: str = None,
        garment_type: str = None,
        expected_pieces: str = None,
        return_nested_pieces=False,
    ):
        """

        This messy shit classifies pieces by shapes in DXF and the compares to passed in pieces and garment type
        /Users/sirsh/Downloads/Maxi Dress - LS All Pocket Options.dxf

        """
        s3 = res.connectors.load("s3")
        root = f"s3://res-data-platform/doc-parse/bodies/{body_key}/dxf"
        did = res.utils.res_hash()
        if isinstance(self._file, str):
            # todo find a proper reg
            did = (
                self._file.split("/")[-1]
                .lower()
                .replace(" ", "_")
                .replace("-", "_")
                .replace("___", "_")
                .replace("__", "_")
            )

        res.utils.logger.info(f"Parsing {did}")

        self._piece_comp_store = ColumnarDataStore(PieceComponentsModel)
        df = self._piece_comp_store.load()
        clu = dict(df.to_pandas()[["piece_component_code", "name"]].values)
        # pass in something to map the names
        self._piece_classifier = BodyPiecesModel(clu=clu)

        def ai(z):
            return LinearRing(np.array(list(z.coords)).astype(int))

        x = self.compact_unfolded_layers
        x["original_geometry"] = x["geometry"]
        x["geometry"] = x["geometry"].map(shift_geometry_to_origin)
        x["geometry"] = x["geometry"].map(scale_shape_by(300))
        outline_layers = x[x["layer"] == 1].reset_index(drop=True).reset_index()
        outline_layers["outline"] = outline_layers["geometry"].map(ai)
        nested = nest_dataframe(outline_layers, geometry_column="outline", buffer=500)
        nested["size_code"] = ""

        nested["outline"] = nested["nested.original.outline"]
        nested["key"] = nested["index"].map(str)
        self._dxf_geom = invert_axis(unary_union(nested["outline"]))

        nested["piece_height_inches"] = (
            nested["original_geometry"]
            .map(shift_geometry_to_origin)
            .map(lambda x: x.bounds[-2])
        )
        nested["piece_width_inches"] = (
            nested["original_geometry"]
            .map(shift_geometry_to_origin)
            .map(lambda x: x.bounds[-1])
        )

        # swap height to be the long end
        H = nested["piece_height_inches"].copy()
        W = nested["piece_width_inches"].copy()
        nested.loc[W > H, "piece_height_inches"] = W
        nested.loc[W > H, "piece_width_inches"] = H
        ############

        nested["is_rectangular"] = nested["original_geometry"].map(
            is_polygon_rectangular
        )
        nested["piece_area_inches"] = nested["original_geometry"].map(
            lambda x: Polygon(x).area
        )
        nested["proportional_size_in_garment"] = (
            nested["piece_area_inches"] / nested["piece_area_inches"].max()
        )

        # symmetries are important for knowing if we might need to duplicate something - for example a full yoke should probably be symmetric
        nested["has_x_symmetry"] = nested["original_geometry"].map(has_x_symmetry)
        nested["has_y_symmetry"] = nested["original_geometry"].map(has_y_symmetry)

        if return_nested_pieces:
            return nested

        im = Image.open(
            io.BytesIO(svg2png(geometry_df_to_svg(nested), scale=0.1))
        ).convert("RGB")
        # describe the pattern pieces and put them in the content store

        expected_pieces_prompt = (
            f"The expected pieces for this garment are {expected_pieces}"
            if expected_pieces
            else None
        )

        res.utils.logger.info(f"Classifying pieces. Please wait...")
        # compute probabilities

        # map the shapes as thumbnails 256, 256 and pass is as we created the samples
        piece_classes = self._piece_classifier.classify_piece_geometries_iterative(
            nested
        )
        piece_classes = piece_classes.join(
            nested[
                [
                    # these props will be useful to downstream classifier
                    "piece_height_inches",
                    "piece_width_inches",
                    "is_rectangular",
                    "piece_area_inches",
                    "has_x_symmetry",
                    "has_y_symmetry",
                ]
            ]
        )

        s3.write(f"{root}/pre_classification.feather", piece_classes)

        piece_classes["comments_and_hints"] = None
        piece_classes.loc[
            piece_classes["is_rectangular"], "comments_and_hints"
        ] = "If the piece is rectangular do not trust the piece codes give but instead use other context such as tech pack notes and piece dimensions to infer the piece name"
        piece_classes.loc[
            piece_classes["is_rectangular"], "piece_code"
        ] = "RECTANGULAR_PIECE (see comments)"
        piece_classes.loc[
            piece_classes["is_rectangular"], "piece_name"
        ] = "RECTANGULAR_PIECE (see comments)"

        # temp fix before clean lookup TODO:
        piece_classes.loc[
            piece_classes["piece_code"] == "SLCUF", "piece_name"
        ] = "Sleeve Cuff"

        res.utils.logger.debug(f"Adding explanations for parts....")

        piece_classes = self.add_part_explanations(piece_classes)
        res.utils.logger.debug(f"Saving {root}/pre_classification.feather")

        res.utils.logger.debug(
            f"Describing the pattern pieces in {piece_classes.columns}"
        )

        def describe_pieces():
            non_rect = (
                lambda x: f"{x['piece_index']}:{x['piece_name']}(W={x['piece_width_inches']}, H={x['piece_height_inches']}) - [NOTES: {x['notes']}]  [Relative Height: {x['relative_piece_height_rank']}]"
            )
            rect = (
                lambda x: f"{x['piece_index']}:{x['piece_name']} (W={x['piece_width_inches']}, H={x['piece_height_inches']}) -  (Rectangular piece)- [NOTES: {x['notes']}]  [Relative Height: {x['relative_rectangular_piece_height_rank']}]"
            )
            return list(
                piece_classes.apply(
                    lambda x: non_rect(x) if not x["is_rectangular"] else rect(x),
                    axis=1,
                )
            )

        # Comment on what all the pattern pieces are likely to be by mapping a name to a DXF index and the pattern piece heights (height is generally more important)
        # - for rectangular pieces list pieces that each piece could be and why - try to compare any height measurements of from the technical notes if available
        # - remember that pockets are generally not rectangular but rather boxy or sometimes irregularly shaped.
        # - very small pieces are often bindings, facings or under pieces for existing pieces of a similar size
        # - for rectangular pieces, the sizes and more importantly the height is the most important property and also the relative height when comparing different pieces.
        # - when in doubt, refer to the Points Of Measure notes (when available) since this gives a great idea about the relative piece sizes
        # - Remember that general names like "all facings" or seams and hems are not valid pieces.
        # - front plackets are usually a similar height/length to the front panels i.e within an inch or two. if you see a skinny piece similar in height to these panels it could be a front placket!
        # - for all height comparisons 'similar' means within one or two inches - DO NOT trust width measurements along when making inferences based on relative sizes

        PROMPT_IMAGE = f"""
        The DXF pattern pieces for a garment of type {garment_type} ar described below.
        A shape classifier predicted the mappings below but the rectangular pieces are likely to be wrong.
        We have also looked at the technical spec and we expect certain pieces to exist. 
        Please observe the relative heights of pieces and suggest succinctly what parts of the garment each of the pieces might be. For examples;
        - a small rectangular piece that is smaller in height to sleeve placket could be the sleeve placket under piece or a front placket may be a similar height to the front/back panel pieces
        - pockets generally are not rectangular but rather boxy and sometimes having a distinctive shape
        - compare heights of pieces in the technical notes to rectangular pieces to infer possible matches - IF and only if the piece is typically rectangular and not already classified in the DXF pieces 
        - Hems and "all facings" in the technical notes are generally not pieces to be considered.
        
        **Predictions**
        ```json
        {json.dumps(describe_pieces())}
        ```
        
        **Technical notes Pieces**
        ```json
        {expected_pieces_prompt}
        ```
    

        """

        resp = self.describe_image(
            im,
            uri=f"{root}/nest_image.{IMAGE_FORMAT}",
            prompt=PROMPT_IMAGE,
        )

        dxf_model = {
            "body_key": body_key,
            "garment_type": garment_type,
            "dxf_id": did,
            # piece areas?
            "piece_predictions": piece_classes.to_dict("records"),
            "pattern_pieces_image": resp["uri"],
            "pattern_pieces_description": resp["description"],
            # some general knowledge
            "description": """This is a description of the pattern pieces provided in the CAD file.         
            Describes the dxf pattern pieces along with logits/probability distributions for classifying pieces
            Note that often DXF pattern pieces only provide one of a pair. For example sleeve related pieces are often given for the right only and we duplicate to get the left
            If there are already multiple pieces of a certain type in the file e.g. because they are not symmetric then it does not need to be duplicated e.g. certain front panels or collar pieces.
            DXf File formats are often in inches""",
        }

        s3.write(f"{root}/classification.json", dxf_model)

        return dxf_model


class PieceLevelInformation(BaseModel):
    piece_name: str
    piece_notes: str
    # may be different sizes for different buttons on difference pieces
    button_sizes: str
    piece_buttons: int = 0
    piece_fusing: str
    # e.g. has left and right or top and bottom pieces
    piece_symmetry_notes: str


class SeamOperations(BaseModel):
    name: str
    measurement_inches: str
    # for example 1 needle or two needle stitches. Look for shorthand like SNTS for single needl top stich or DNTS for double needle top stitch.
    needles_notes: str
    other_notes: str


class PointsOfMeasure(BaseModel):
    name: str
    measurement_inches: str
    other_notes: str


class TopLevelGarmentDetails(BaseModel):
    garment_name: str
    garment_code: str
    garment_base_size: str
    all_garment_sizes: str

    piece_details: List[PieceLevelInformation]
    seam_operations: List[SeamOperations]
    points_of_measure: List[PointsOfMeasure]
    image_sources: List[str]


""" questionable value
        #notes may mentioned a double piece or a panel might be split by a placket or other piece leading two double the piece count
        expected_piece_count: int = 1
"""

# notes: i made per piece buttons a strong to see if when group it can do something other than just add 0 or a total (the down side of structure)
MODEL_STRING = """class PieceLevelInformation(BaseModel):
            #name of a specific piece - if use this to describe general pieces like "all facings" or general regions of the garment you should add a note that it is not a true piece 
            piece_names: List[str]
            piece_notes: str
            per_piece_buttons: str = "0"
            piece_button_notes: str
            #may be different sizes for different buttons on difference pieces
            button_sizes: str
            #this is inferred from general knowledge about garment pieces
            likely_piece_fusing: str
            #if the piece group is made of multiple pieces you can mention so here
 
            #e.g. has left and right or top and bottom pieces
            piece_symmetry_notes: str
 

        class SeamOperations(BaseModel):
            name: str
            measurement_inches: str
            #for example 1 needle or two needle stitches
            needles_notes: str
            other_notes: str

        class PointsOfMeasure(BaseModel):
            name: str
            measurement_inches:str
            other_notes: str

        class ResultSet(BaseModel):
            garment_name: str
            garment_code: str
            garment_base_size: str
            all_garment_sizes: str

            #list all physical pieces and omit the following from piece list; hems, pleats, seams, general regions and categories like "bodice" or "all pieces" are NOT specific enough to list as pieces
            piece_details : List[PieceLevelInformation]
            #seams have measurements and names as well as mentions of needle types e.g. SNTS (Single needle top stitch) or DNTS (Double Needle Top stitch)
            seam_operations: List[SeamOperations]
            points_of_measure: List[PointsOfMeasure]
            image_sources : List[str]
            garment_sketch_image: str = None
            #the total number of pockets is important since we need to name them for their function. If there is only one pocket just call it a patch pocket
            total_number_of_pockets: int = None
            
            """


class ParsingAgent:
    PLAN = f"""
        Please parse structured information from the content provided. 
        You will be given knowledge extracted from scanned images or parsed PDF. 
        Your objective is to parse out information at three levels; (1) Garment level, (2) Piece level (3) Seam level operations and (4) Specific points of measure (POM).
        
        You are provided a structured Json/Pydantic schema below to fill in the details as per the context.
        The images have already been parsed so you do not usually need to describe images but if you feel your answer is poor, call available functions only once per image uri. (its wasteful to keep describing the same image over and over again.)
        
        **Pydantic Response Model**
        ```python
        {MODEL_STRING}
        ```
        
        You should response in the format if possible or just use a simper json format if the answer does not led itself to this structure
    """

    def combine(self, parts):
        P = f"""
           We parsed separate information from different pages of a technical spec below in the DATA section. 
           Please combine the notes into the common Json response schema which is given below.
           It is important to retain all the pieces that are referenced in these notes. 
           Be comprehensive in listing ALL specifically mentioned physical pieces, all seam operations and ALL points of measure.
           
           **DATA**
           ```json
            {json.dumps(parts)}
           ```
  
           
           **Json Response Schema**
           ```python
           {MODEL_STRING}
           ```
           
           Be careful to expand out and list each piece one by one.
           Check your work so that no important information is omitted. It is very important to fully describe each piece in the technical document.

        """

        R = json.loads(self(P))

        if "processed_pieces_data" not in R:
            # de-ref
            res.utils.logger.warn(f"Deref key {R.keys()}")
            d = list(R.values())[0]
            res.utils.logger.warn(f"New keys {d.keys()}")
            if "piece_details" in d:
                R = dict(d)

        return R


class TechpackPageParser:
    def __init__(
        self, index, page_text, page_scan, page_images, agent=None, key=None, **kwargs
    ):
        self.page_number = index
        self.page_text = page_text
        self.page_scan = page_scan
        self.page_images = page_images

        self.key = key

        self._scan_description = None
        self._sketch_descriptions = []

        self._agent = agent

        self._built = False
        self._data = None

    # def export(self):
    #     data = {
    #         "image_descriptions": self._sketch_descriptions,
    #         "scan_description": self._scan_description,
    #         "page_text": self.page_text,
    #     }
    #     uri = f"{self.key}/parsed.json"
    #     res.connectors.load("s3").write(uri, data)

    def restore(self):
        # uri = f"{self.key}/parsed.json"
        # data = res.connectors.load("s3").read(uri)
        # self._sketch_descriptions = data.get("image_descriptions")
        # self._scan_description = data.get("scan_description")
        # self.page_text = data.get("page_text")
        # # this is saved in deep parsed
        uri = f"{self.key}/page_scan.json"
        self._data = res.connectors.load("s3").read(uri)
        return self._data

    @retry(wait=wait_fixed(5), stop=stop_after_attempt(3), reraise=True)
    def concise_parse_page(self, i=0):
        from res.observability.dataops import parse_fenced_code_blocks
        import yaml

        #   - separately for each front and back panel, observe if it is completely split end to end by a placket or not
        im = self.page_scan
        res.utils.logger.debug(
            f"We have {len(self.page_images)} sub images in this page"
        )
        # this is temporary - we need to actually seek the sketch out image by image if it exists and at least it would be safer to merge all image descriptions here

        P = """
                If there is no garment sketch or images just provide a brief summary what the page is about e.g. any top level headings etc.
                For any sketches or photos, your job is to list every single separate piece in the front and back sketch. Observe the following as examples if relevant to the style of the garment;

                - the garment may have a top and bottom region e.g. jumpsuits or just a top part e.g. a shirt or just a bottom part like a skirt or pants. It may be an accessory piece like a hat or bag
                - you should describe it based on relevant body areas which may or may not be relevant for all garment types 
                - the major garment regions in general are; (front bodice, back bodice, sleeve/arm, neck/collar, bottom area)?
                - what are the full list of pieces and their connectivity in the distinct regions relevant to the garment 
                - if the garment has a top part (not relevant for skirts and pandas for example), observe if the front or back regions contain a yoke piece (being careful not to confuse a back yoke that is visible from the front or vice versa)
                - also observe if the primary front or back panels are split by another piece such as a button placket or if they are a single solid piece - this is important
                - generally, what pieces are connected to other pieces?
                - what are the relative lengths and widths of connected pieces?
                - what accessory parts are attached to other pieces and on which sides?
                - are any pieces perfectly rectangular strips such as tabs and plackets
                - never refer to "side" panels which are not used
                - what type of garment is it? - briefly describe the style.
        """

        sketch_texts = []
        page_images = self.page_images
        if not len(page_images):
            page_images = [im]
        for j, sketch_image in enumerate(page_images):
            s = self.describe_image(
                sketch_image, prompt=P, uri=f"{self.key}/page{i}/image{j}.png"
            )
            sketch_texts.append(s)

            res.utils.logger.debug(s)

        P = """
        Extract all the tabular data into a valid yaml format. Be careful to parse ALL measurements (assumed in inches) and table values and omit any numeric indexes from lists.
        When extracting measurements capture the measurement name, value in inches and any comments
        If there is no interesting tabular data return a simple message in yaml format to say so
        """
        d = self.describe_image(im, prompt=P, uri=f"{self.key}/page-scans/image{i}.png")
        a = parse_fenced_code_blocks(d["description"], select_type="yaml")
        if a:
            a = yaml.safe_load(a[0])
        else:
            a = {
                "error": "there was an error loading this part of the document - there may be no tabular data of interest"
            }
        a["image_uri"] = d["uri"]
        # taken from above, the sketch description if relevant
        a["sketch_description"] = sketch_texts
        return a

    @retry(wait=wait_fixed(5), stop=stop_after_attempt(2), reraise=True)
    def describe_image(self, image: Image, prompt: str, uri: str):
        """
        give a numpy or PIL image or a uri to an image - describe it

        by writing the image to s3 we can ask the agent to describe it

        name: is optional for a handle to the image otherwise everyone writes to the same place which is fine for testing
        prompt: what are we trying to extract
        """

        s3 = res.connectors.load("s3")
        s3.write(uri, image)

        res.utils.logger.debug(f"describing {uri}  ")
        if not s3.exists(uri):
            raise Exception(f"That file {uri} was not saved")

        desc = describe_visual_image(
            s3.generate_presigned_url(uri), question_relating_to_url_passed=prompt
        )

        if "sorry, but I can't assist with that request." in desc:
            res.utils.logger.warn(
                f"<<<<<< FAILED TO DO THE IMAGE THING - RETRY ONCE >>>>>>>>"
            )
            raise Exception("Failed - to get a response from the agent")

        return {"uri": uri, "description": desc}

    def _describe_inline_image(self, image, uri):
        P = """
        Describe the image relating to the technical design of a garment and please.
        Specifically, there are two types of data you will encounter which are visual sketches and also tabular data.
        (1) [Garment sketches]
            - if the image is a front and/or back sketch of a garment please briefly describe its overall style and carefully list out all the distinct pieces you observe and where in the layout you observe them
            - Your goal is to create a complete manifest of all visible pieces required to make the garment
            - You should call attention to accessory pieces like "pockets", "plackets", "(sleeve) tabs", "flaps" that are visible in the sketch and specify what region they belong to (sleeve, bodice, neck, bodice, bottoms)
            - you should note if the front or back panels are completely split (left and right) or not. For example the front panel could be split top to bottom in two by placket or otherwise.
            - be specific when naming any primary or accessory piece and say what part of the garment the piece belongs to i.e. bodice, sleeve, neck, bottom , front, back, left, center, right etc.
            - sometimes the same part is visible front and back and you only need to mention it in the main area. For example you might see the top of the back yoke coming over slightly to the front or different sides of sleeves or collars
            - if a piece seems to be COMPLETELY split in half by another piece it is safer to assume its made up of two pattern pieces e.g. a left and a right 
            - if a piece such as a tab, placket, binding etc appears perfectly rectangular in shape you state that explicitly and comment on its size relative to the garment or another piece!!
        (2) [Tabular data]
            - If the image shows tabular data you should be comprehensive in listing tabular data and measurements, observing the table header names
            - Respond in a JSON format
        """

        d = self.describe_image(image, P, uri)
        res.utils.logger.debug(d)
        return d

    @retry(wait=wait_fixed(2), stop=stop_after_attempt(5), reraise=True)
    def export(self, add_text=False, add_sketches=True):
        """
        prompt with the actual text and the  export
        """
        from res.observability.dataops import parse_fenced_code_blocks
        import yaml

        tx_lines = self.page_text.replace("\u2007", "").split("\n")

        P = f"""The following text has been parsed from the document. Please organize the text into sections by reviewing the image. Use a YAML format with headed sections and data. Do not abbreviate for brevity because we need ALL the details
        
        **Text lines**

        ```json
        {json.dumps(tx_lines)}
        ```
        """

        j = self.describe_image(
            self.page_scan, P, uri=f"{self.key}/page_scan.{IMAGE_FORMAT}"
        )
        y = parse_fenced_code_blocks(j["description"], select_type="yaml")
        if y:
            y = y[0]
        try:
            y = yaml.safe_load(y)
        except Exception as ex:
            res.utils.logger.warn(
                f" >>>>>>>>>>> Failed to parse the YAML ({ex}) - trying again <<<<<<<<<<<<<"
            )
            raise Exception("Failed to parse yaml string")
        y["uri"] = j["uri"]
        y["images"] = []
        if add_sketches:
            try:
                for i, image in enumerate(self.page_images):
                    res.utils.logger.debug(f"describing image {i}")
                    uri = f"{self.key}/page_images/image_{i}.{IMAGE_FORMAT}"
                    d = self._describe_inline_image(image, uri)
                    y["images.append"] = d
            except Exception as ex:
                res.utils.logger.warn(f"Failing to describe image in page:> {ex}")
        if add_text:
            y["raw_parsed_text"] = tx_lines

        self._data = y
        res.connectors.load("s3").write(f"{self.key}/page_scan.json", y)
        return y

    # def build(self):
    #     """
    #     Look for any sketches
    #     """

    #     if self._built:
    #         return

    #     P = """
    #         Describe the image relating to the technical design of a garment and please.
    #         Specifically, there are two types of data you will encounter which are visual sketches and also tabular data.
    #         (1) [Garment sketches]
    #             - if the image is a front and/or back sketch of a garment please briefly describe its overall style and carefully list out all the distinct pieces you observe and where in the layout you observe them
    #             - Your goal is to create a complete manifest of all visible pieces required to make the garment
    #             - You should call attention to accessory pieces like "pockets", "plackets", "(sleeve) tabs", "flaps" that are visible in the sketch and specify what region they belong to (sleeve, bodice, neck, bodice, bottoms)
    #             - you should note if the front or back panels are completely split (left and right) or not. For example the front panel could be split top to bottom in two by placket or otherwise.
    #             - be specific when naming any primary or accessory piece and say what part of the garment the piece belongs to i.e. bodice, sleeve, neck, bottom , front, back, left, center, right etc.
    #             - sometimes the same part is visible front and back and you only need to mention it in the main area. For example you might see the top of the back yoke coming over slightly to the front or different sides of sleeves or collars
    #             - if a piece seems to be COMPLETELY split in half by another piece it is safer to assume its made up of two pattern pieces e.g. a left and a right
    #         (2) [Tabular data]
    #             - If the image shows tabular data you should be comprehensive in listing tabular data and measurements, observing the table header names
    #             - Respond in a JSON format
    #     """
    #     for i, p in enumerate(self.page_images):
    #         uri = f"{self.key}/page_images/image_{i}.{IMAGE_FORMAT}"

    #         d = self.describe_image(p, P, uri)
    #         res.utils.logger.debug(d)
    #         self._sketch_descriptions.append(d)

    #     # - If there are no details for a category or piece, simply omit the attribute and do not mention zero, null or empty values.  this may create problems for tables
    #     P = f"""
    #        Describe this scan of a garment design technical document (page {self.page_number}).
    #        Please pay particular attention to information in the following categories (if relevant) and be detailed in describing facts about names and measurements that you observe;
    #        Specifically, try to list;
    #        1. Top level product information such as garment name, size scale, base size etc.
    #        2. Piece-level information i.e. mention of specific pieces required to make the garment and include what part of the garment the piece belongs to in the name i.e. bodice, sleeve, neck, bottom
    #        3. List the number and size of buttons and trims on each piece
    #        4. Seam operations or Point of measure data - list the names and measurements.

    #        - Please produce itemized lists of pieces, measurements, operations etc as observed.
    #        - Respond in a Yaml format.

    #     """
    #     uri = f"{self.key}/page_scan.{IMAGE_FORMAT}"
    #     self._scan_description = self.describe_image(self.page_scan, P, uri)
    #     res.utils.logger.debug(self._scan_description)
    #     self._built = True

    # def deep_parse(self, include_scan=False):
    #     content = {
    #         "document_scan_notes": (self._scan_description if include_scan else None),
    #         "auxillary_parsed_text": self.page_text,
    #         "sketches_notes": self._sketch_descriptions,
    #     }

    #     content = {k: v for k, v in content.items() if v}

    #     res.utils.logger.debug(
    #         f"<<<<<<<<<<<<<<<<< PERFORMING DEEP PARSE FOR PAGE DATA ({content.keys()}) >>>>>>>>>>>>>>>>>"
    #     )

    #     ####
    #     ##   HERE WE NEED TO MAKE SURE WE LEAVE NOTHING BEHIND PER PAGE
    #     ####

    #     #         If you do not have a value simply omit it rather than writing expressions like null, 0, unknown, not specified etc.

    #     request = f"""
    #      Please structure the various parsed content into the structure provided or if the response is not suitably structure just use a simple json format like {{'response': CONTENT}}.
    #      List all pieces that are mentioned instead of describing higher level groups or regions like bodice or "all facings".
    #      The sketch notes provide a good idea about how the garment is constructed and this can be augmented with piece information from the technical notes.

    #      **Content**
    #      ```json
    #        {json.dumps(content)}
    #      ```

    #     """
    #     self._data = self._agent(request)
    #     uri = f"{self.key}/parsed.json"
    #     res.utils.logger.debug(f"Writing partial result {uri}")
    #     res.connectors.load("s3").write(uri, self._data)

    #     return self._data


class TechpackParser:
    def __init__(
        self, uri, dxf_file=None, key=None, min_size=(300, 300), flip_images=False
    ):
        res.utils.logger.info(f"Opening {uri}...")
        s3 = res.connectors.load("s3")

        self._dxf_file = dxf_file
        self._tech_file = uri

        self.key = key or res.utils.res_hash()
        self._root = f"s3://res-data-platform/doc-parse/bodies/{self.key}"

        if "s3://" not in uri:
            s3.upload(uri, f"{self._root}/input_tech_doc.pdf")

        pdf_path = (
            io.BytesIO(open(uri, "rb").read())
            if not "s3://" in uri
            else s3.as_stream(uri)
        )

        # self._agent = ParsingAgent(allow_functions=False)

        pdf_reader = PyPDF2.PdfReader(stream=pdf_path)
        self._parsed_text = []

        self._num_pages = len(pdf_reader.pages)

        # here we add the text parsed from each page
        for page_num in range(len(pdf_reader.pages)):
            page = pdf_reader.pages[page_num]
            text = page.extract_text()
            self._parsed_text.append(text)

        self._page_images = []
        self._page_image_info = []
        with fitz.open(stream=pdf_path) as pdf_document:
            # https://github.com/pymupdf/PyMuPDF/issues/385
            for page_num in range(pdf_document.page_count):
                page = pdf_document.load_page(page_num)
                iminfo = page.get_image_info()

                self._page_image_info.append(iminfo)
                # here we add separate images on each page
                image_list = []
                for ii, img in enumerate(page.get_images(full=True)):
                    # -0 im transform used below
                    transform = iminfo[ii].get("transform")

                    xref = img[0]
                    base_image = pdf_document.extract_image(xref)
                    image_bytes = base_image["image"]
                    image = Image.open(io.BytesIO(image_bytes))

                    if image.size[0] < min_size[0] or image.size[1] < min_size[1]:
                        continue
                    # third comp for rotation
                    if flip_images or transform and "-" in str(transform[2]):
                        image = ImageOps.flip(image)
                    image_list.append(image)

                self._page_images.append(image_list)

        # these are the full page scans of the page
        if "s3://" in uri:
            self._image_pages = s3.process_using_tempfile(uri, fn=convert_from_path)
        else:
            self._image_pages = convert_from_path(uri)

        assert len(self._image_pages) == len(
            self._page_images
        ), "we expect the same number of pages as page data"
        assert len(self._parsed_text) == len(
            self._page_images
        ), "we expect the same number of pages as page data"

        self._pages = [
            TechpackPageParser(
                i,
                page_text=self._parsed_text[i],
                page_scan=self._image_pages[i],
                page_images=self._page_images[i],
                # # presume reset is best
                # agent=ParsingAgent(allow_functions=False),
                key=f"{self._root}/{i}",
            )
            for i in range(len(self._page_images))
        ]
        res.utils.logger.info(f"All good")

    def __getitem__(self, index) -> TechpackPageParser:
        return self._pages[index]

    def __iter__(self):
        for p in self._pages:
            yield p

    def build(self):
        for p in self:
            p.build()

    def export(self):
        for p in self:
            p.export()
        res.utils.logger.info(f"exported to {self._root}")

    def restore(self):
        self._parsed = []
        for p in self:
            self._parsed.append(p.restore())

        uri = f"{self._root}/page-parse-raw/parsed.json"
        self._data = res.connectors.load("s3").read(uri)
        res.utils.logger.info(f"restored")

        # load dxf file object - sub class the other one to make it a StaticMetaOneDxfFile

    def _clear_history(self):
        s3 = res.connectors.load("s3")
        f = self.list_files()
        res.utils.logger.debug(f"Purging {len(f)} files")
        for i in f:
            s3._delete_object(i)
        res.utils.logger.debug("done with purge")

    def extract_garment_summary_from_data(self, data):
        """"""

        P = f"""Please summarize the garment from the data and response in the json format using the mode below

            **Response Model**
            ```
            ResponseModel(BaseMode):
                garment_name: str
                garment_type: str
                garment_description_brief: str
                garment_sizes: str
                garment_base_size: str
            ```
            
            **DATA**
            {json.dumps(data)}
        """

        d = self._agent(P, prompt="respond in a json format")
        return json.loads(d)

    def extract_pieces_from_data(self, data):
        """"""

        P = f"""Given the parsed data from the technical spec, your job is to extract a full list of pieces required to make the garment.
            You should start from the visible pieces in the sketch as reference and then pick out possible facings and under pieces from the notes.
            1. You will observe that certain pieces have seam operations and points of measure (in inches)
            2. you will observe that certain pieces have notes alluding to "double" facing or stitching that require "under" pieces 
            3. create a flat list if all pieces that are mentioned. Do not include generic descriptions of pieces like "all facings" but please include every physical piece that is mentioned in the notes
            4. if there are pieces "with" other pieces, list those pieces separately
            5. if inferring heights you can often assume that pieces connected to other pieces have similar heights for example the front placket is probably the same height as the front panel(s)
            6. A bodice is not a piece. A front part of the bodice is made up up panels and these panels may be split into left and right if separated by a center front placket

            Provide a list of pieces and observe any height and width measurements given for the piece if relevant and if the piece is likely to be fused or not. Use the following schema
            
            **Response Model**
            class PieceInfo(BaseModel):
                #if a piece has a left and right you can group it as one pieces but otherwise keep pieces separate
                #dont include hems or generic terms like "all facings" in the list of garment pieces and instead of "bodice" describe the specific panels etc that comprise it
                piece_names: List[str]
                #known measurements
                height_and_width_notes: str
                connected_to_pieces: List[str]
                #if you do not know the height and width, comment on if its a similar size to another piece
                comparative_size_notes: str
                has_facing_or_double_under_part: bool
                has_fusing: bool
                #if a front panel is connected to a center front placket you need to split it into left and right
                is_divided_left_and_right: bool
                
            class GarmentInfo(BaseModel):
                garment_name: str
                garment_category: str
                pieces: List[PieceInfo]
                comments: str
                #percent
                confidence: float
            **DATA**
            {json.dumps(data)}
        """

        d = ask(P, as_json=True)
        d = json.loads(d)
        res.utils.logger.debug(f" <<<<< determined all pieces")
        res.utils.logger.debug(d)
        return d

    def par_read(self, prompt=None, from_cache=False, num_workers=4):
        """
        we read the document in parallel - if the prompt is passed we could guide to some specific information but generally we have a standard prompt for garment development
        """
        s3 = res.connectors.load("s3")
        path = f"{self._root}/reader_cached.json"

        res.utils.logger.info(
            f"Reading {self._num_pages} pages in total using {num_workers} workers..."
        )
        if from_cache and s3.exists(path):
            return s3.read(path)

        import concurrent.futures
        from functools import partial

        def f(i, tech_file, dxf_file, key):
            tp = TechpackParser(tech_file, key=key, dxf_file=dxf_file)
            try:
                return tp[i].concise_parse_page(i)
            except Exception as ex:
                # raise  # While testing at least
                return {"error": f"failed to parse page {i} - {ex}"}

        f = partial(f, tech_file=self._tech_file, dxf_file=self._dxf_file, key=self.key)

        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            future_results = [executor.submit(f, i) for i in range(self._num_pages)]
            results = [
                future.result()
                for future in concurrent.futures.as_completed(future_results)
            ]

        res.connectors.load("s3").write(path, {"data": results})

        return results

    def deep_parse(
        self,
        refine_pieces=True,
        index_filter=None,
        clear_history=False,
        from_check_point=False,
    ):
        s3 = res.connectors.load("s3")

        if from_check_point:
            res.utils.logger.info(
                "<<<<<<<<<<<<<<<<< RESTORING CHECKPOINT >>>>>>>>>>>>>>>>>"
            )
            self.restore()
        else:
            if clear_history:
                self._clear_history()

            res.utils.logger.info(
                "<<<<<<<<<<<<<<<<< PARSING ALL PAGES >>>>>>>>>>>>>>>>>"
            )
            self._data = None
            self._parsed = []
            for i, p in enumerate(self):
                self._parsed.append(p.export())

        res.utils.logger.info(
            "<<<<<<<<<<<<<<<<< COMBINING KNOWLEDGE FROM ALL PAGES >>>>>>>>>>>>>>>>>"
        )
        # self._data = self._agent.combine(self._parsed)
        # we may process this later
        DATA = {"page_details": self._parsed}
        DATA["processed_pieces_data"] = self.extract_pieces_from_data(DATA)
        res.utils.logger.debug("ADDING: Garment summary")
        DATA["summary"] = self.extract_garment_summary_from_data(DATA)
        self._data = DATA  # self.model_from_parsed_text()

        # adding notes of where the sketches are is useful for reviewing
        # self._data["sketch_notes"] = all_sketches

        self._data["file_uri_given"] = self._tech_file
        self._data["tech_doc_file_uri"] = f"{self._root}/input_tech_doc.pdf"
        self._data["dxf_input_data"] = f"{self._root}/dxf/input.dxf"
        self._data["processed_plt_file"] = f"{self._root}/meta_one.plt"
        # save it
        uri = f"{self._root}/page-parse-raw/parsed.json"
        res.utils.logger.info(f"Writing result {uri}")
        s3.write(uri, self._data)

        if self._dxf_file:
            res.utils.logger.info(
                "<<<<<<<<<<<<<<<<< PARSE & CLASSIFY DXF >>>>>>>>>>>>>>>>>"
            )
            self.process_dxf()

            # build the PLT file and update that too

        if refine_pieces:
            res.utils.logger.info(
                "<<<<<<<<<<<<<<<<< REFINE PIECE NAMES >>>>>>>>>>>>>>>>>"
            )
            self.refine_piece_names(map_resonance_names=True)

        # refine sizes

        return self._data

    def model_from_parsed_text(self):
        texts = {}
        sketches = {}
        for i, p in enumerate(self):
            texts[f"Page_{i}_text"] = p.page_text
            sketches[f"Page_{i}_text"] = p._sketch_descriptions

        P = f"""Please compile exhaustive details on pieces, points of measure and seam operations using the data below;

        ** ALL tech pack TEXT **
        ```json
        {json.dumps(texts)}
        ```
        **Sketch descriptions (may contain duplicates)**
        ```json
        {json.dumps(sketches)}
        ```
        
        """
        return json.dumps(self.ask(P))

    def describe_all_piece_info(self):
        P = f"""
         Below is a collection of piece information extracted from multiple pages. Please compile the information removing duplicates to describe the pieces required to make this garment.
         Some general guidelines for construction are below
         
         **Construction guide**
         ```
         {CONSTRUCTION_WISDOM}
         ```
         
         **Page info**
         {
             json.dumps(self._parsed)
         }
        """

        return json.loads(self.ask(P))

    def process_dxf(self):
        dxf = StaticMetaOneDxfFile(self._dxf_file)
        if f"s3://" not in self._dxf_file:
            res.utils.logger.info(
                f"uploading file {self._dxf_file} -> {self._root}/dxf/input.dxf"
            )
            res.connectors.load("s3").upload(
                self._dxf_file, f"{self._root}/dxf/input.dxf"
            )
        pcs = self._data["processed_pieces_data"]

        # sue the KEY that is passed in (could also be the garment code in the the pack)
        dxf.parse_for_meta_one(
            self.key,
            garment_type=self._data["summary"]["garment_name"],
            expected_pieces=pcs,
        )

        dxf.export_static_meta_one(f"{self._root}/meta_one", material_codes=["PIMA7"])

    def ask(
        self,
        question: str,
        include_refined_pieces=False,
        prompt=None,
        response_format=None,
        channel_context=None,
        user_context=None,
    ):
        """

        logs:
        ask with full context is useful except if we have function calling and an ability to load on demand
        - need to think about what makes sense to preload
        """
        # details = self._data
        # dxf_uri = f"{self._root}/dxf/classification.json"
        # dxf_report = res.connectors.load("s3").read(dxf_uri)

        P = f"""
        Use the data from the Garment technical notes and the DXF pattern analysis the following user question and respond in
        
        **Question**
        ```text
        {question}
        ```
        
        -------------

        """

        # **Garment technical notes**
        # ```json
        # {json.dumps(details)}
        # ```

        # **DXF Pattern pieces analysis**
        # ```json
        # {json.dumps(dxf_report)}
        # ```

        # here we can add on the refined stuff to answer the questions

        if include_refined_pieces:
            uri = f"{self._root}/processed/refined_pieces.json"
            P += f"""
        
            ** Refined piece analysis **
            ```json
            {json.dumps(res.connectors.load("s3").read(uri))}
            ```

            """
        from res.learn.agents.BodyOnboarder import BodyOnboarder

        return BodyOnboarder()(
            P,
            prompt=prompt,
            response_format=response_format,
            channel_context=channel_context,
            user_context=user_context,
        )  # ask mode

    @retry(wait=wait_fixed(5), stop=stop_after_attempt(3), reraise=True)
    def refine_rectangular_piece_names(self, map_resonance_names=False):
        piece_details = self._data  # .get("processed_pieces_data")

        dxf_uri = f"{self._root}/dxf/classification.json"
        dxf_report = res.connectors.load("s3").read(dxf_uri)

        P = f"""
            For each piece mentioned in the notes, find the height and width in one of the following ways;
            1. If the piece or its binding or facing is described in the DXF pattern pieces, use the height and width. Pay close attention to the `pattern_pieces_description` which provides deeper post analysis
            2. If the height/width is listed in the Points of Measure in the technical notes use that height and width
            3. Finally, if the height OR width are not mentioned anywhere, use the garment sketch information to compare it to another piece to infer BOT the height and width
            
            **Response Model**
            PieceInfo(BaseModel):
                piece_name_from_notes: str
                dxf_piece_index_if_known: int
                #give the number in inches
                piece_height_inches: float
                #if the pieces is fully connected to another piece, it may share the same height
                piece_as_described_in_sketch_and_any_other_pieces_its_connected_to: str
                #give the number in inches
                piece_width_inches: float
                #the height and width must be determined - when comparing the height and width they must be within an inch and half (comparing DXF pieces to tech notes) in order to be a match
                comments_on_piece_match: str
                
            AnalysisResponse(BaseModel): 
                piece_dimension_info: List[PieceInfo]
                
            **[A] Pieces from technical notes: contains all pieces, points of measure, seam operations etc.**
            ```json
            {json.dumps(piece_details)}
            ```
            
            **[B] DXF Pattern pieces analysis: contains pre classification of rectangular and non rectangular pattern shapes**
            ```json
            {json.dumps(dxf_report)}
            ```
               Your job is to follow the above strategy to infer the approximate height and width of all pieces. Exhaustively list every piece, placket, facing, binding mentioned in the notes!
                                        
              """

        res.utils.logger.debug(P)

        data = json.loads(
            BodyOnboarder(allow_functions=False)(
                P,
                prompt="Respond in a json format and use functions as needed ",
            )
        )

        return data

    def get_resonance_codes(self, data):
        """
        as a separate task we are trying to make the resonance code components
        """

        p = PieceRefiner()
        data = p.refine_piece_data(data)
        data = json.loads(data)

        uri = f"{self._root}/processed/refined_pieces_with_codes.json"
        res.utils.logger.info(f"Writing result {uri}")

        res.connectors.load("s3").write(uri, data)

        # this is totally shit - just trying to play out the workflow with stages while we refine
        # data["id_mapping"] = get_piece_codes(data)

        # res.connectors.load("s3").write(uri, data)

        return data

    # retry on failure
    @retry(wait=wait_fixed(5), stop=stop_after_attempt(3), reraise=True)
    def refine_piece_names(
        self, map_resonance_names=False, min_confidence_threshold=90
    ):
        # its possible we can summarize this first
        piece_details = self._data  # .get("processed_pieces_data")

        dxf_uri = f"{self._root}/dxf/classification.json"
        dxf_report = res.connectors.load("s3").read(dxf_uri)

        P = f"""
         You are a piece classification agent that fully identifies pattern pieces to make a garment by comparing the technical notes about pattern pieces to the DXF pattern pieces data.
         Follow this strategy:
         1. Solve the rectangular pieces sub problem 
         - what pieces in the notes could be the rectangular DXF pattern pattern pieces based on approx measurement matching e.g. within an inch or two in height. Its fine to propose multiple possibilities at first
         - a very good tip is if you dont know how tall a piece is, you can check the sketch notes and see what other piece height it compares to from which you can infer its height
         -  Pay close attention to the `pattern_pieces_description` in the DXF pattern piece notes, which provides deeper post analysis
         2. Once you propose this mapping of rectangular pieces, you can classify ALL pieces (keeping in mind that in some rare instances the pattern piece might not exist). If there are two front panel (FTPNL pieces assume left and right)
         3. When mapping pieces, you need to list ALL pieces mentioned in the notes and only link them to DXF pattern piece indexes that exist
         4. It is efficient to group pieces by the same prefix and then list orientations and their mappings to DXF pattern pieces (e.g. e.g. a sleeve left and right panel or a back yoke top and under(facing) piece). 
         You can generally assume that a symmetrical pieces (left/right/facing/top/under) uses the same DXF index as its partner piece unless an extra binding piece and corresponding measurements are mentioned in the technical notes
         
        --------------------    
        
        ** Response Model ***
        
        class PossibleRectangularPieceInfo(BaseModel):
            name_in_notes: str
            #this is an important measurement   - you can infer from a sketch or similar piece how tall another piece is.  
            height_in_notes_or_inferred_from_sketch_and_similar_pieces: str
            width_in_notes: str
            possible_dxf_pattern_piece_index: List[int]
            all_possible_dxf_pattern_pieces_heights_and_widths_description: str
            
        
        class PieceInfo(BaseModel):
            #you should mention the piece without its orientation i.e. without left, right, top, under etc and use the mappings below to map each orientation to a dxf pattern piece
            #include bias and binding pieces needed to finish the garment if they are explicitly mentioned in the notes
            full_input_piece_name_base: str
            #if we have a better name for the piece
            alternate_piece_name: str
            
            #map one or more orientations of the piece to DXF pattern pieces using name matching or rectangular piece inferences - map all orientations onto left/right/top/under for example default, self etc can map to top (TP) while Fac/facing/under/double can map to under (UN)
            #sleeve pieces usually need a left and a right even if they are already duplicated as top and under
            #map str->int
            pieces_mapped_to_valid_dxf_piece_index: dict
            #measurement notes: especially for rectangular pieces, you should list the actual height and width measurements you observed for the DXF and/or pattern pieces and the "points of measurement" in the tech pack notes (if relevant)
            #add height and width measurements from PoM if known (especially for rectangular pieces)
            rectangular_height_and_width_piece_measurements_from_pom: str
            rectangular_height_and_width_piece_measurement_from_dxf:str
            #Only for rectangular pieces, both the Height and the Width measurements need to match for this to be true. The Height is particularly important. 
            is_rectangular_piece_height_and_width_from_pom_matching_dxf_within_approx_one_inch: bool = False
            is_fused: bool
         
         class GarmentConstruction(BaseModel):
            garment_name: str
            garment_code: str
            garment_category: str
            
            #this is your checksum; make sure you include all of the pieces in the notes in this list and true/false if there is a classified pattern piece in the DXF pattern piece that could match
            all_pieces_including_bindings_facings_plackets_in_notes_and_dxf_pattern_status: dict
            
            #this is a list of pieces that are rectangular in the DXF file that have no names. It can not include pieces that are classified in the DXF
            #the purpose of this list is to find unmapped pieces in the notes that could explain the pieces that are rectangular in the DXF pattern pieces
            #it is wrong to include a piece mapping where the height is not explained by the notes. AI often gets the frustratingly wrong!
            #do not choose pockets as possible rectangular pieces
            potential_rectangular_pieces: List[PossibleRectangularPieceInfo]
            
            pieces: List[PieceInfo]
            #quality of answer leading to confidence depends on three things
            # i. accounting for every single dxf pattern piece indexes (i.e. you need to map every index to some piece), 
            # ii. mapping pieces mentioned in the the list `all_pieces_including_bindings_facings_plackets_in_notes_and_dxf_pattern_status` - pay particular attention to plackets and accessory pieces which could be missed
            good_matching_confidence_percentage: float
            confidence_notes: str
            
        **[A] Pieces from technical notes: contains all pieces, points of measure, seam operations etc.**
        ```json
        {json.dumps(piece_details)}
        ```
        
        **[B] DXF Pattern pieces analysis: contains pre classification of rectangular and non rectangular pattern shapes**
        ```json
        {json.dumps(dxf_report)}
        ```
        
        --------------------

        Your job is to follow the above strategy to create a full list of pieces needed to make the garment while mapping their DXF file index.
                                   
        """

        res.utils.logger.debug(P)

        data = json.loads(
            BodyOnboarder(allow_functions=False)(
                P,
                prompt="Respond in a json format and use functions as needed ",
            )
        )

        uri = f"{self._root}/processed/refined_pieces.json"
        res.utils.logger.info(f"Writing result {uri}")
        res.connectors.load("s3").write(uri, data)
        conf = data.get(
            "good_matching_confidence_percentage",
            data.get("GarmentConstruction", {}).get(
                "good_matching_confidence_percentage"
            ),
        )
        res.utils.logger.debug(f"confidence {conf}")
        if conf < min_confidence_threshold:
            res.utils.logger.debug(
                f" <<<<<<<<<<<<<<< retry for low confidence >>>>>>>>>>>>>"
            )
            raise Exception(
                f"The confidence is to low at {data['good_matching_confidence_percentage'] } < 90"
            )

        if map_resonance_names:
            data = self.get_resonance_codes(data)
        return data

    def get_size_mappings(self):
        """ """
        r = self.ask(
            """First identify the sizes of the garment in the technical spec and then get the airtable size codes and airtable ids for each. Determine which size is the base size .
            Use the response format below
            
            **Response Model**
            ```python
            
            class SizeInfo(BaseModel):
               size_input: str
               resonance_size_code: str
               resonance_size_name:str            
               airtable_id: str
               is_base_size: bool
               
            class AllSizesInfo(BaseModel):
              sizes: List[SizeInfo]
            ```
            """
        )
        return json.loads(r)

    def get_points_of_measure(self):
        """ """

        res.utils.logger.debug(
            "Requesting and formatting points of measure. Pease wait..."
        )
        r = self.ask(
            """Please list all the points of measures given in the technical spec
               Response in a Json format using the response model below.
                         
            **Response Model**
            ```python
            
            class PoM(BaseModel):
               notes: str
               name: str
               measurement_inches: float
               
            class PointsOfMeasure(BaseModel):
              measurements: List[PomM]
            ```
            """
        )
        return json.loads(r)

    def __call__(self, question):
        data = self.par_read(from_cache=True)
        b = BodyOnboarder()
        prompt = f"""
            
            You are an agent that helps compile quality and comprehensive specs to make a garment
            Use the data below to answer the user's question.
            If you are asked for links to images, check the s3 uris in te data and look for something that might match the description.
            
            **Question**
            ```text{           question             }```
            
            **data**
            ```json
            {json.dumps(data)}
            ```
            
        """
        return b.ask(prompt)
        # return self.deep_parse()

    def list_files(self):
        s3 = res.connectors.load("s3")
        return list(s3.ls("s3://res-data-platform/doc-parse/bodies/EP-6000-V0"))

    def dxf_pattern_pieces_nested_image(self):
        uri = f"{self._root}/dxf/nest_image.{IMAGE_FORMAT}"
        return res.connectors.load("s3").read(uri)

    def dxf_pattern_pieces_classification(self):
        uri = f"{self._root}/dxf/classification.json"
        return res.connectors.load("s3").read(uri)

    def sketch_image(self):
        path = self._data["sketch_notes"][0]["uri"]

        return res.connector.load("s3").read_iamge(path)

    @retry(wait=wait_fixed(0.5), stop=stop_after_attempt(2), reraise=True)
    def get_static_meta_one(self):
        """
        this is an experimental compiler - there is some API or Airtable that  needs to be updated with a compiled payload
        here we use the agent to compile the payload and post
        """
        import stringcase
        from res.connectors.airtable import AirtableConnector

        class StaticMetaOne(BaseModel):
            name: str
            body_code: str
            notes: str
            status: str
            technical_notes: str
            dxf_file: str
            plt_file: str
            sketches: List[str]
            sketch_description: str
            points_of_measure_notes: str
            seam_operations_notes: str
            sizes: str
            base_size: str
            brand: str

        d = self.ask(
            """
        Please compile the information from the body and save in the json response format below. When asked for notes you need to be exhaustive in copying the notes from the source into the response model.
        
        **Response Model
        class StaticMetaOne(BaseModel):
            name: str
            body_code: str
            notes: str
            status: str
            #the s3 uri or link to the technical notes - this is usually a pdf file
            technical_notes: str
            #the s3 uri or link to the input dxf file e.g. dxf/input.dxf on s3
            dxf_file: str
            #the generated link to the plt file if known
            plt_file: str
            #the s3 uri or link to the sketch images - these are usually front and back "flats". these are not the page scans.
            sketches: List[str]
            #exhaustive description of the garment from the notes
            sketch_description: str
            #exhaustive formatted notes about PoMs in the body with full measurements
            points_of_measure_notes: str
            #exhaustive formatted notes about the seam operations required, with full measurements
            seam_operations_notes: str
            sizes: str
            base_size: str
            brand: str
        
            - verify the piece codes by mapping the piece names to full piece codes
            - fill in the other details requested with as much detail as possible
        
        """,
            response_format={"type": "json_object"},
            prompt="respond in json format",
        )

        d = json.loads(d)
        sd = {
            "sizes": d["sizes"],
            "base_size": d["base_size"],
        }
        sd = get_size_info(sd)

        res.utils.logger.debug(sd)
        for a in ["points_of_measure_notes", "seam_operations_notes", "sizes"]:
            d[a] = str(d[a])
        d = StaticMetaOne(**d).dict()

        d["size_scale"] = sd["size_scale_id"]
        d["base_size"] = sd["base_size_id"]

        airtable = res.connectors.load("airtable")
        s3 = res.connectors.load("s3")
        # load piece info
        id_mapping = s3.read(
            "s3://res-data-platform/doc-parse/bodies/EP-6000-V0/processed/refined_pieces_with_codes.json"
        )["id_mapping"]
        id_mapping = {k: v for k, v in id_mapping.items() if v is not None}
        d["pieces"] = list(id_mapping.values())

        tab = airtable["appa7Sw0ML47cA8D1"]["tblZKEb9PmDQZEWoH"]

        for a in ["technical_notes", "plt_file", "dxf_file", "sketches"]:
            v = d[a]
            if isinstance(v, str):
                v = [v]
            d[a] = [
                {"url": s3.generate_presigned_url(f)} for f in v if f and "s3://" in f
            ]
            d[a] = d[a] if len(d[a]) else None

        D = {stringcase.titlecase(k).strip(): v for k, v in d.items()}
        res.utils.logger.debug(D)
        # check existing
        try:
            pred = AirtableConnector.make_key_lookup_predicate(
                [D["Body Code"]], "Body Code"
            )
            records = tab.to_dataframe(fields=["Body Code", "Name"], filters=pred)

            if len(records):
                D["record_id"] = records.iloc[0]["record_id"]
            tab.update_record(D)
        except:
            import traceback

            res.utils.logger.warn(traceback.format_exc())

        return D

    def resonance_pieces_from_dxf_pattern_pieces(self):
        P = """
        From the DXF pattern pieces notes, please list all pieces and their mappings to DXF pieces and determine the Resonance code names for the pieces. Do not omit any of the 15 pattern pieces and note some pieces can be reused more than once and some pieces as listed might be formed of multiple pieces e.g. top and under part
        """
        res.utils.logger.debug(P)
        return json.loads(self.ask(P))

    def determine_piece_symmetries(self, pieces=["Back Yoke"]):
        P = f"""
        'Your job is to determine how many of copies of the pattern pieces for the {pieces} pieces are needed to make the garment. 
        Note its symmetry and check out the sketch and the tech pack notes to determine how many pieces we need; 1,2 or 4? List all references to the back yoke pieces (including mentions do facing or "double" pieces), seam operations and points of measure before answering'
        """
        return self.ask(P)

    def determine_all_pieces(self, map_res_names=False):
        """
        investigation into the ability to determine rectangular pieces and then classified pieces to make a final determination
        """

        P = """Please collect all the details about the body from the ingested notes and respond in the JSON format given by the model below. 
        ** Response Model **

        ```json
        class PieceInfo(BaseModel):
            input_name: str
            #e.g. DRSSLPNLLT using the format for codes being product taxonomy|part|component|orientation
            resonance_piece_code: str
            notes: str
            number_of_buttons: str
            dxf_file_piece_index: int
            
        class StaticMetaOne(BaseModel):
            #garment name
            name: str
            #format should be two characters, hyphen, number e.g. XX-0000
            body_code: str
            #you can lookup input sizes and map them to (resonance) size_codes and airtable ids
            available_sizes_info: List[dict]
            #you can lookup the input base size and map them to resonance size codee and id
            base_size_info: dict
            input_technical_file_uri: str
            input_dxf_file_uri: str
            pattern_sketch_uri: str

            #list the names of all the pieces that you see for the `bodice` region if any. These include yokes and front and back panels which may be split into left and right pieces on some garments 
            bodice_region_piece_names: List[str]
            #list the names of all the pieces that you see for the `neck` region if any
            neck_region_piece_names: List[str]
            #list the names of all the pieces that you see for the `sleeves` region if any (left and right)
            sleeves_region_piece_names: List[str]
            #list the names of all the pieces that you see for the `bottom` region if any
            bottoms_region_pieces: List[str]
            # you should put accessory pieces in the proper region if you can. Use `accessory_pieces` for any extra accessory pieces e.g. pockets, plackets, binding pieces etc ONLY IF you do not know what region they belong to
            # if you do know what region they belong to put them in one of `bodice_region_pieces`, `neck_region_pieces`, `bottoms_region_pieces`, `sleeves_region_pieces`
            accessory_piece_names:List[str]
            # reproduce the full garment sketch verbatim
            full_sketch_description: str
            #list of dxf  index map to piece names
            dxf_file_pieces: dict
        ```

        """

        a = self.ask(
            P,
            prompt="Use the context from the question to fill in as much detail as possible. Use any functions you need.",
        )

        if map_res_names:
            res.utils.logger.info(
                f"<<<<<<<<<<<<<<< Doing the extra task of mapping the the piece names>>>>>>>>>>>>>>>"
            )
            P = f"""Given that we have determine the pattern pieces types, now confirm the number of symmetries needed for each piece and then map the resonance piece code to each piece and its symmetries (orientations).
            The piece classifications as inferred are given below but you can use other functions to complete the task.
            
            **Input Data: Piece Inferences**
            ```json
            {json.dumps(a)}
            ```
            
            """
            a = self.ask(
                P,
                prompt="Use the context from the question to fill in as much detail as possible. Use any functions you need.",
            )

        return a

    def determine_neck_and_collar_pieces(self):
        x = self.ask(
            """From the following configurations of neck and collar pieces, which is the most likely from the sketch and notes and pattern pieces;
        1. A neck and collar stand 
        2. A neck collar over and under and a collar stand over and under
        3. A neck band
        4. Option 1 with an adjoining center front placket running down the body
        5. Option 2 with an adjoining center front placket running down the body

        When you have made your choice you can look up the naming conventions for resonance piece codes and create a full set of required pieces
        - use the sketch notes rather that describing the image again
                
        Please respond in the json format given by the model below
        
        **ResponseModel(BaseModel):
            #map the piece index in dxf to the piece its used for
            dxf_pattern_pieces_index_used_in_neck_region: dict
            explanation_for_choice: str
            #make your final choice based on the evidence gathered
            choice: int
            resonance_piece_codes_required_for_neck_region: List[str]
        """,
            response_format={"type": "json_object"},
            prompt="Respond in the json format given",
        )
        return json.loads(x)

    def determine_back_yoke_pieces_from_multiple_choices(self):
        x = self.ask(
            """From the following configurations of yoke, which is the most likely from the sketch and notes and pattern pieces;
        1. Perhaps the pattern piece requires mirroring to make left and right sides of a complete yoke and then doubling to create the under facing for the self piece
        2. Perhaps the pattern piece in the DXF file is enough to make the entire garment without mirroring or duplicating pieces
        3. Perhaps the pattern piece must be mirrored and there is no need to double the self pieces for top and under
        4. Perhaps the pattern piece must only be duplicated to create a double self (top and under) and there is no need to mirror left and right pieces

        When you have made your choice you can look up the naming conventions for resonance piece codes and create a full set of required pieces
        - use the sketch notes rather that describing the image again
                
        Please respond in the json format given by the model below
        
        **ResponseModel(BaseModel):
            #map the piece index in dxf to the piece its used for
            dxf_pattern_pieces_index_used_bodice_region: dict
            explanation_for_choice: str
            #make your final choice based on the evidence gathered
            choice: int
            resonance_piece_codes_required_for_yoke_region: List[str]
            
        """,
            response_format={"type": "json_object"},
            prompt="Respond in the json format given",
        )
        return json.loads(x)

    def determine_bodice_components_from_multiple_choices(self):
        a = self.ask(
            """From the following configurations of bodice, which is the most likely from the sketch and notes;
        1. A Back panel, yoke and a front panel
        2. A back panel, yoke and a left and right front panel
        3. A back panel, double yoke and a left and right front panel
        4. Option 1 with a left pocket on the chest and a center front placket
        5. Option 2 with a left pocket on the chest and a center front placket
        6. Option 3 with a left pocket on the chest and a center front placket
        7. Other: explain

        When you have made your choice you can look up the naming conventions for resonance piece codes and create a full set of required pieces
        - use the sketch notes rather that describing the image again
                
                
        Please respond in the json format given by the model below

        **ResponseModel(BaseModel):
            #map the piece index in dxf to the piece its used for
            dxf_pattern_pieces_index_used_bodice_region: dict
            explanation_for_choice: str
            #make your final choice based on the evidence gathered
            choice: int
            resonance_piece_codes_required_for_bodice_region: List[str]
            #map each piece to a summary of dimensions given in the Points Of Measure
            points_of_measure_verification_for_rectangular_bodice_pieces: dict
            
        """,
            response_format={"type": "json_object"},
            prompt="Respond in the json format given",
        )
        return json.loads(a)

    def determine_sleeve_region_components_from_multiple_choices(self):
        a = self.ask(
            """From the following piece symmetry configurations of sleeves, which is the most likely from the sketch and notes?
                1. A sleeve panel left and right with no additional accessory pieces
                2. A sleeve panel left and right with a sleeve tab and a single-piece sleeve placket
                3. A sleeve panel left and right with a sleeve tab and a double-piece sleeve placket (double-piece means it has an underside /facing(s) possibly alluded to in the tech pack notes)
                
                - When you have made your choice you can look up the naming conventions for resonance piece codes and create a full set of required pieces.
                - You should analyse the DXF pattern pieces to understand what pieces are candidates to be used in this context and check for specific descriptions of sleeve components in the notes
                - Please respond in the json format given by the model below
                - use the sketch notes rather that describing the image again
                
                **ResponseModel(BaseModel):
                    comprehensive_summary_of_all_sleeve_pieces_mentioned_in_notes: str
                    description_of_type_of_plackets: str
                    #map the piece index in dxf to the piece its used for
                    dxf_pattern_pieces_index_used_sleeve_region: dict
                    explanation_for_choice: str
                    #make your final choice based on the evidence gathered
                    choice: int
                    resonance_piece_codes_required_for_sleeve_region: List[str]
                    #map each piece to a summary of dimensions given in the Points Of Measure
                    points_of_measure_verification_for_rectangular_sleeve_pieces: dict
                    
        """,
            response_format={"type": "json_object"},
            prompt="Respond in the json format given",
        )
        return json.loads(a)

    def determine_all_components_from_multiple_choices(self):
        p = {
            "Sleeves Region": self.determine_sleeve_region_components_from_multiple_choices(),
            "Bodice Region": self.determine_bodice_components_from_multiple_choices(),
            "Yoke Region drill down": self.determine_back_yoke_pieces_from_multiple_choices(),
            "Neck and Collar Region": self.determine_neck_and_collar_pieces(),
        }

        P = f"""
         Below we ran an analysis on some of the regions in the body that are sometimes tricky to classify. 
         We have chosen the most likely pieces required to make the garment.
         
         Please complete the analysis by reviewing the tech notes and DXF pattern pieces (particularly the rectangular pieces) and make a final choice about what the complete set of pieces needed is.
         It is very important that you list every piece required to make the garment along with the resonance piece codes. 
         You must confirm (a) that you have used ALL pieces in the DXF pattern pieces and that you have verified the properly re-named rectangular pieces mappings agree with the Points of Measurements for those pieces e.g inferred Tabs, Plackets, etc.
         
         Use the response model and data below to respond in a json format...
         
         **REGION DATA**
         ```json
         {json.dumps(p)}
         ```
         
         **RESPONSE MODEL**
         class PieceMapping(BaseModel):
            piece_name_or_code: str
            dxf_piece_index: List[int]
            
         #efficiently collect a group of pieces that have the same base parts
         class PieceGroupInfo(BaseModel):
            description: str
            #for example if we have left and right pieces, just list the piece once and add the symmetries below
            piece_type_base: str
            #for example left, right, top, under etc
            piece_orientations: str
            #all resonance piece codes - use a function to lookup the piece names and make sure that the correct Resonance (our company) component codes are used to construct the full piece code
            resonance_codes:str
            #map the piece index in dxf to the piece its used for. for example map the dxf file to a left or right piece or all the yoke pieces (left/right/top/under) to a pattern piece
            dxf_pattern_pieces_index_to_piece_code_mapping: List[PieceMapping]
            
         class ResponseModel(BaseModel):
           #map each piece to a summary of dimensions given in the Points Of Measure - you must check this first before confirming the pattern piece mappings is logical
           points_of_measure_verification_for_rectangular_pieces: dict
           
           pieces : List[PieceGroupInfo]


        """

        a = self.ask(
            P,
            response_format={"type": "json_object"},
            prompt="Respond in the json format given",
        )
        d = {"partials": p, "answer": json.loads(a)}

        uri = f"{self._root}/processed/region_analysis.json"
        res.utils.logger.info(f"Writing result {uri}")
        res.connectors.load("s3").write(uri, d)

        return d

    @retry(wait=wait_fixed(0.5), stop=stop_after_attempt(2), reraise=True)
    def experimental_shape_maker(self):
        a = self.ask(
            """
       
        **Response Model**
        class Pocket(BaseModel):
          notes: str
          points_wkt: str 
          
            search_ingested_techpack_content_for_body to find the notes for the patch pocket which are in inches. all of these measurements are numbered in the text. 
            There is enough information there to infer the symmetric shape of the patch pocket. it has a height at center which is different to its height at edges providing a pointed tip to the pocket at the base.
            Please provide the outline as a polygon using WKT format  

            """,
            prompt="Respond in the json format given and use the notes to find the measurements",
            response_format={"type": "json_object"},
        )

        from shapely.wkt import loads

        return loads(json.loads(a)["points_wkt"])


@retry(wait=wait_fixed(0.5), stop=stop_after_attempt(2), reraise=True)
def get_piece_codes(pdata=None):
    """
    lots of hacks in here just to test the airtable update while we get smarter at knowing what pieces
    """
    airtable = res.connectors.load("airtable")
    s3 = res.connectors.load("s3")
    res.utils.logger.info("fetch piece mapping")
    if not pdata:
        pdata = airtable.get_table_data(
            "appa7Sw0ML47cA8D1/tbl4V0x9Muo2puF8M",
            fields=["Name", "Generated Piece Code"],
        )
        pdata = dict(pdata[["Generated Piece Code", "record_id"]].values)

    def generate_codes():
        data = s3.read(
            "s3://res-data-platform/doc-parse/bodies/EP-6000-V0/processed/refined_pieces_with_codes.json"
        )
        garment_category_code = data["garment_category_code"]

        for p in data["pieces"]:
            part_code = p["part_code"]
            component_code = p["component_code"]
            the_part = f"{garment_category_code}{part_code}{component_code}"
            all_orientation_code_mapped_to_dxf_indices = p[
                "all_orientation_code_mapped_to_dxf_indices"
            ]
            suffix = "-S"
            if len(all_orientation_code_mapped_to_dxf_indices) > 1:
                index = 0
                for k, v in all_orientation_code_mapped_to_dxf_indices.items():
                    suffix = "-S"
                    v = v[0]
                    if k == "UN" or component_code == "CUF" or part_code == "NK":
                        suffix = "-BF"
                    index += 1

                    # temp
                    if the_part == f"DRSBKYKE":
                        part = f"{the_part}{k}LF{suffix}"
                        yield part
                        part = f"{the_part}{k}RT{suffix}"
                        yield part
                    else:
                        part = f"{the_part}{k}{suffix}"
                        yield part
            else:
                k = ""
                if len(all_orientation_code_mapped_to_dxf_indices) > 0:
                    first_key = list(all_orientation_code_mapped_to_dxf_indices.keys())[
                        0
                    ]
                    if first_key in ["LF", "RT"]:
                        k = first_key
                if component_code == "CUF" or part_code == "NK":
                    suffix = "-BF"

                if the_part == f"{garment_category_code}FTPNL":
                    part = f"{the_part}LF{suffix}"
                    yield part
                    part = f"{the_part}RT{suffix}"
                    yield part
                else:
                    part = f"{the_part}{k}{suffix}"
                    yield part

    pcs = list(generate_codes())
    return dict(zip(pcs, [pdata.get(p) for p in pcs]))


def get_size_info(input_data):
    airtable = res.connectors.load("airtable")

    res.utils.logger.info("Loading size info from res.meta")
    ssdata = airtable.get_table_data(
        "appa7Sw0ML47cA8D1/tblS3AkeOdjaTTUTj", fields=["Sizes String", "Name"]
    )
    ssdata = dict(ssdata[["Sizes String", "record_id"]].values)

    sdata = airtable.get_table_data(
        "appa7Sw0ML47cA8D1/tblInQ3Ixcpczjo39", fields=["SIZE_SCALE", "Name"]
    )
    sdata = dict(sdata[["Name", "record_id"]].values)

    data = {"size_scales": ssdata, "sizes": sdata}
    agent = BodyOnboarder(allow_functions=False)
    P = f"""
        please map the input sizes to a size scale id and map the base size to a size id. The data provides the mappings from which you can infer the size scale and then pick the base size id in that size scale.

        **INPUT SIZES**
        ```json
        {json.dumps(input_data)}
        ```
        **DATA**
        ```json
        {        json.dumps(data)         }
        ```
        **Response Model**

        class SizeInfo(BaseModel):
            base_size_id: str
            size_scale_id: str

        
    """
    rdata = agent(
        P,
        response_format={"type": "json_object"},
        prompt="response with the json response",
    )

    return json.loads(rdata)
