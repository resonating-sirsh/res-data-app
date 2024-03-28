from typing import Any
import openai
from res.utils import logger
import typing
import json
from res.observability.dataops import (
    describe_function,
    FunctionDescription,
)
from res.observability.io import VectorDataStore, ColumnarDataStore, EntityDataStore
from res.observability.entity import (
    AbstractVectorStoreEntry,
    AbstractEntity,
    NpEncoder,
)
from res.observability.io.EntityDataStore import Caches
import res
from res.utils import get_res_root
import os
from res.utils.secrets import secrets
import pandas as pd
from res.observability.types import ResonanceSizesModel, PieceComponentsModel
from res.media.parsers.content_parsers import ContentParser
from res.media.images.geometry import *
from res.media.images.providers.dxf import DxfFile
import numpy as np
from PIL import Image

try:
    from res.learn.models.BodyPiecesModel import BodyPiecesModel
except:
    res.utils.logger.info(f"Cannot include the DL model - BodyPiecesModel")

DEFAULT_MODEL = "gpt-4-1106-preview"  #  "gpt-4"  # has a 32k context or 8k


plans = {
    "find_sleeve_placket_pairs": """
    the sleeve placket (under piece) is rectangular and a similar size (maybe an inch or so smaller) than the sleeve placket (top piece). For example the top might be a dog house sleeve placket.
    By looking at the the DXF pattern piece data, identify the sleeve placket and than infer which other piece is the under sleeve placket given that it must fit within the dimensions of the upper part""",
    # in this case we are trying to use common sense and awareness of what is already in the dxf file. e.g. dont duplicate collars or front panels that have pairs already
    "pairing_objects": """What pieces in the DXF file are likely to require duplication. You may consult the sketch of the garment. Certain pieces are expected to be symmetric and if they are not we probably need to duplicate""",
}


class BodyOnboarder:
    PLAN = """
     You are an intelligent agent that parses documents and general content to extract specific information about instruction to manufacture a garment for the company Resonance. 
     You will have access to functions and stores to search for content about a specific body and also look up general data and codes related to Resonance e.g naming conventions.
     You will use combinations of text, structured data and images to fill out a specific format that you will given to describe a body and all its separate pieces
     
     Below following is a Pydantic Model that can guide your task. 
     You should determine the body information and also as much details as you can for each piece in the body.
     You can start by filling int he top level garment information and progressively fill in more details about the pieces as you learn more.
          
     Follow this plan:
     1. Always start by searching the ingested garment content since the task is to interpreter the ingested content with the help of other utilities
     2. Images have been described but you can re-inspect or "describe" visual images using the urls if you have them if you think you can learn more from any of the images
     3. If necessary augment your knowledge by looking up general sewing knowledge or resonance specific terms and codes using the supplied function
     4. If you identify interesting codes it may be useful to use the entity store to learn more
     5. Respond in a JSON format using the model below as a guide when appropriate OR just answer any general questions in the format {'response': RESPONSE} if the structure does not apply
     6. You should add your comments on strategy you used to the field `agent_strategy_comments` and include your confidence in your answer.
     
     ```Pydantic Model
        class SeamOperation(BaseModel):
            #seams are usual described as operation type, a measurement in inches
            name: str
            needles: str
            size_inches: int
    
        class PieceInput(BaseModel):
            #piece information,  angle_offset is for grain, bias, stretch etc. and number of needles like Single/double needle top stich DNTS/SNTS
            name: str
 
            #a piece is described by multiple codes which can be looked up by a function. For example a Back Yoke piece would have components ['BK','YKE']
            piece_component_code:  typing.List[str]
            #partial : 5char component
            #use the orientation codes that we have and list them if possible - the orientation codes can be looked up by a function
            symmetries: typing.List[str] = []
            #enumeration
            fusing_type: str = None
            seam_allowance_inches: int = None
            #button offset and button spacing is interesting
            button_count: int = 0
            #context - known button enumeration
            button_size: str = None
            seam_operations: typing.List[SeamOperation] = []
            angle_offset: int = 0
            piece_description: str = ""
            index_of pattern_piece_in_dxf_file: typing.Optional[int] = None
            #piece connectivity
                
        class GarmentInput(BaseModel):
            #the garment "Body" is a collection of pieces
            name: str
            body_code: str
            brand: str
            category: str
            gender: str = None
            size_scale: str = None
            #sample or base size
            base_size: str = None 
            #all sizes
            supported_fabrics : typing.List[str] = []
            pieces: typing.List[PieceInput] = []
            possible_piece_names: typing.List[str] = []
            sketch_image_uri: str = None 
            pattern_pieces_layout_image_uri: str = None
            agent_strategy_comments: str = None
            #TODO
            #target due date
            #moment information
            #poms/seams
            #avatar
     ```
    """

    # may use a sub model or sub agent to parse piece information
    def __init__(
        self,
        cache_name: str = None,
        allow_functions: bool = True,
        allow_only_inspection_calls: bool = False,
        use_specific_functions: typing.List[typing.Callable] = None,
    ):
        self._slack = res.connectors.load("slack")

        self._content_store = VectorDataStore(
            AbstractVectorStoreEntry.create_model(
                name="body_content", namespace="meta"
            ),
            create_if_not_found=True,
        )
        self._entity_store = EntityDataStore(
            AbstractEntity,
            cache_name=cache_name or Caches.QUEUES,
        )

        self._sizes_store = ColumnarDataStore(ResonanceSizesModel)
        self._piece_comp_store = ColumnarDataStore(PieceComponentsModel)

        if allow_functions:
            self._built_in_functions = [
                describe_function(self.search_ingested_techpack_content_for_body),
                # describe_function(self.get_resonance_general_size_codes),
                # describe_function(
                #     self.get_general_construction_info_for_rectangular_pieces
                # ),
                describe_function(self.get_garment_piece_component_codes_lookup),
                # describe_function(
                #     self.get_rules_for_pattern_piece_symmetry_and_duplication
                # ),
                describe_function(self.describe_visual_image),
                describe_function(self.get_dxf_pattern_pieces_description),
                # describe_function(self.refinement_advice),
                # self._entity_store.as_function_description(name="entity_key_lookup"),
            ]
        else:
            self._built_in_functions = None
        if allow_only_inspection_calls:
            self._built_in_functions = [describe_function(self.describe_visual_image)]
        if use_specific_functions:
            self._built_in_functions = [
                describe_function(f) for f in use_specific_functions
            ]

        df = self._piece_comp_store.load()
        clu = dict(df.to_pandas()[["piece_component_code", "name"]].values)
        # pass in something to map the names
        try:
            self._piece_classifier = BodyPiecesModel(clu=clu)
        except:
            res.utils.logger.warn(
                f"could not setup the Body pieces model in the Body onboarder"
            )

    def refinement_advice(
        self, context: str, partial_solution_json: str = None, **kwargs
    ):
        """
        You can call this function to ask for advice; state your issue and also provide your partial solution in the schema your were provided.

        **Args**
            context: explain your confusion or lack of data
            partial_solution_json: your current partial solution
        """

        F = f"""
        You have reported that you cannot satisfactorily complete the task. You said: {context}. You have constructed a partial solution below
        
        **Partial Solution**
        ```json
        {partial_solution_json}
        ```
        
        Because you can trust the pattern piece names for non rectangular pieces, you really just need to make sure that you are making reasonable assumptions for the rectangular pieces. 
        Some pieces will be much larger than others in height. By checking the heights of other pieces or heights in the points of measure, you can find pieces that make sense.
        The DXF pattern piece for the height would have a height that is close to the heights mentioned in points of measure (within and inch or so) 
        If you also check the sketch notes, you might have some idea about how big some of the rectangular pieces are relatively to each other
        Finally, if you are sure there is no mentioned of a piece in the notes, you can confidently say that the piece is indeterminate.  
        """

        if not partial_solution_json and partial_solution_json != "":
            raise Exception(
                "You must provide your partial solution to get further advice"
            )
        # res.utils.logger.debug(F)#
        return F

    def invoke(
        cls,
        fn: typing.Callable,
        args: typing.Union[str, dict],
        # allowing a big response but we probably dont need to summarize - if we do make sure to use a model that can take it
        max_response_length=int(5 * 1e5),
    ):
        """
        here we parse and audit stuff using Pydantic types
        reconsider max response length for large context
        """

        res.utils.logger.info(f"fn={fn}, args={args}")

        args = json.loads(args) if isinstance(args, str) else args

        # the LLM should give us this context but we remove it from the function call
        for sys_field in ["__confidence__", "__parameter_choices__"]:
            if sys_field in args:
                logger.debug(f"{sys_field}  = {args.pop(sys_field)}")

        try:
            data = fn(**args)
        except Exception as ex:
            m = {
                "FunctionCallStatus": "Error",
                "message": "You called the function incorrectly or the function encountered a fatal problem",
                "error_message": repr(ex),
            }
            res.utils.logger.warn(m)
            return m

        """
        experimental - refactor out
        we should come up with a cheap way to summarize
        the idea here is you are "forcing" the interpreter to summarize but you should not. how to?
        
        """
        if len(str(data)) > max_response_length:
            return cls.summarize(
                question=cls._question,
                data=data,
                model=DEFAULT_MODEL,
                max_response_length=max_response_length,
            )

        return data

    def add_content(self, uri: str, body_key: str, **options):
        """
        We might add a doc key as body key so we can filter content
        how do we manage what is the "latest" content for the body. TBD
        """
        # parse the pdf file and store all the stuff in the database

        res.utils.logger.info(f"Parsing content at {uri} for body key {body_key}")
        records = [
            AbstractVectorStoreEntry(**d)
            for d in ContentParser().extract_images_and_text_from_pdf_doc(uri)
        ]
        self._content_store.add(records)

        return self._content_store

    def get_general_construction_info_for_rectangular_pieces(
        self, question: str = None
    ):
        """
        Rectangular pieces are ambiguous so use this function to learn about different uses.
        You can then use other information such as the dimensions of the pieces (height and width) and also knowledge about the pieces that are known to be in the garment to infer what some rectangular pieces might be

        **Args**
            question: context to ask about dxf file

        """

        f = """
           **About Rectangular pieces
           
           Rectangular pieces can be plackets, bindings, bias pieces, facings, ruffles, belts, bands etc. The length of the piece can tell something about its function 
           by either checking its dimensions in the points of measure or seeing how similar it is to either pieces (similar length or width).
           
           
           1. Sleeve Plackets (Under piece or facing): A sleeve placket may have a secondary part called a sleeve under placket. This might not be explicitly mentioned in the points of measure. The tech packs notes might mention a sleeve placket but not that it is made of two pieces
             The sleeve placket under piece is smaller than the over placket which may have a "Dog House" or other shape. 
             The sleeve placket under piece is relatively small (smaller than the sleeve placket)
             You can refer to a facing as an "Under" piece is the resonance parlance
            2. Pockets are not usually rectangular. They may have some shape and if they are regularly shaped they will probably be more square and boxy looking 
        
        """

        return f

    def get_dxf_pattern_pieces_description(self, question: str = None):
        """
        Describes the dxf pattern pieces along with logits/probability distributions for classifying pieces
        Note that often DXF pattern pieces only provide one of a pair. For example sleeve related pieces are often given for the right only and we duplicate to get the left
        If there are already multiple pieces of a certain type in the file e.g. because they are not symmetric then it does not need to be duplicated e.g. certain front panels or collar pieces.
        DXf File formats are often in inches

        **Args**
            question: context to ask about dxf file

        """
        s3 = res.connectors.load("s3")

        return s3.read(
            "s3://res-data-platform/doc-parse/bodies/EP-6000-V0/dxf/classification.json"
        )

    def parse_dxf(
        self,
        uri: str,
        body_key: str = None,
        garment_type: str = None,
        expected_pieces: str = None,
        return_nested_pieces=False,
    ):
        """
        /Users/sirsh/Downloads/Maxi Dress - LS All Pocket Options.dxf

        """
        # from res.learn.models import BodyPiecesModel
        from res.learn.optimization.nest import nest_dataframe
        from res.flows.meta.ONE.geometry_ex import geometry_df_to_svg
        from res.media.images.geometry import (
            is_polygon_rectangular,
            has_x_symmetry,
            has_y_symmetry,
        )
        from cairosvg import svg2png
        import io

        def ai(z):
            return LinearRing(np.array(list(z.coords)).astype(int))

        # add DXF document info: the image of the thing and the number pieces and their areas + and the top 10 likely piece distribution
        # add all of this to the content store as a bundle describing what we can see about the pieces - one time
        res.utils.logger.info(f"Parsing and nesting pieces in DXF {uri}")

        dxf = DxfFile(uri)

        self._dxf = dxf

        did = res.utils.res_hash(uri.encode())
        x = dxf.compact_unfolded_layers

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

        im = Image.open(io.BytesIO(svg2png(geometry_df_to_svg(nested), scale=0.1)))
        # describe the pattern pieces and put them in the content store
        garment_type_prompt = (
            f" The garment is a {garment_type}" if garment_type else ""
        )
        expected_pieces_prompy = (
            f"The expected pieces for this garment are {expected_pieces}"
            if expected_pieces
            else None
        )

        res.utils.logger.debug(f"Describing the pattern pieces")
        resp = ContentParser().describe_image(
            im,
            name=f"pattern_pieces_{did}",
            prompt=f"These are pattern pieces in the DXF - {garment_type_prompt} - observe the relative sizes and shapes and suggest succinctly what parts of the garment they may be. . {expected_pieces_prompy}",
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

        piece_classes["comments_and_hints"] = None
        piece_classes.loc[
            piece_classes["is_rectangular"], "comments_and_hints"
        ] = "If the piece is rectangular do not trust the piece codes give but instead use other context such as tech pack notes and piece dimensions to infer the piece name"
        piece_classes.loc[
            piece_classes["is_rectangular"], "piece_code"
        ] = "RECTANGULAR_PIECE (see comments)"

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

        s3 = res.connectors.load("s3")

        s3.write("s3://res-data-platform/samples/json/dxf_report.json", dxf_model)

        return dxf_model

    # add the piece component areas and descriptions in the entity store - average area, width and height of each piece - maybe variance

    def garment_construction_knowledge(self, question: str):
        """
        Lookup general knowledge about garment construction that may be nuanced.

        """

        return """
            pattern pieces are sewn together using different types of stitching operations. 
            The operations properties may depend on the type of material/fabric.
            Pieces may require extra fusing to strengthen them
            We can see pattern pieces in CAD files like in the DXF format. Sometimes we may have to mirror and duplicate provided pattern to complete the symmetries for top, under, left, right etc.
            The Bodice is a core part of the garment usually broken into front and back panels. Front panels may be split into left and right
            Collars normally come with collar stands and each of these can in turn appear in pairs of top and under parts
            Sleeve plackets under can be rectangular and they can pair with doghouse plackets. for pairing the under might be about an inch smaller as it looks the roof
        """

        # TODO create an image of standard construction

    def search_ingested_techpack_content_for_body(
        self, questions: typing.List[str] = None, body_code: str = None
    ):
        """
        Use this to search for knowledge related to the body in the form of text and images e.g. parsed from PDF files or emails
        You may see references to images and you can re-inspect those images using the describe visual image function
        You this function repeatedly to mine for all the information required to complete the description of how to manufacture the garment

        **Args**
          questions: ask one or more question to query the content store
          body_code: use as a hint to filter for a specific body (optional)
        """

        return res.connectors.load("s3").read(
            "s3://res-data-platform/doc-parse/bodies/EP-6000-V0/page-parse-raw/parsed.json"
        )
        # if not questions:
        #     questions = [
        #         "please tell me as much details about the pieces, construction and measurement in the body "
        #     ]

        # return self._content_store.run_search(
        #     questions,
        #     _extra_fields=[
        #         "id",
        #         "text",
        #         "doc_id",
        #         "image_content_uri",
        #         "text_node_type",
        #     ],
        # )

    def get_resonance_general_size_codes(
        self,
        question: str = None,
        size_data_parsed_from_content: str = None,
        base_or_sample_size: str = None,
        context: str = None,
    ):
        """
        Use this function to map generic size names to Resonance company codes and ids. This is not specific to a garment
        You can pass in the sizes and base size specified in the data and then by loading all the data for our sizes
        return the best match for each size. For example if the data have sizes like Large and Medium you can use our L,M scale
        or if the size has 1,2,3 or Size 1, Size 2 in the name you can match those sizes

        **Args**
          question: a question to trigger a search for garment sizes e.g. load all or lookup specific size codes
          size_data_parsed_from_content: The size info parsed from a document that you are enquiring about
          base_or_sample_size: the base size (aka sample size) should be given by the technical specification document
          context: explain why you are asking
        """

        if size_data_parsed_from_content is None:
            raise Exception("Please specify the sizes that you are looking up")
        question = f"{question or ''} {size_data_parsed_from_content or ''} {f'base size->{base_or_sample_size}' }"
        data = self._sizes_store.load().to_dicts()

        res.utils.logger.debug(f"{question=} {context=}")

        response = self.ask(
            f"""Please provide list of sizes with the codes and airtable ids using the sizes in the lookup data 
                 You can pass in the sizes and base size specified in the data and then by loading all the data for our sizes
                 return the best match for each size. For example if the data have sizes like Large and Medium you can use our L,M scale
                 or if the size has 1,2,3 or Size 1, Size 2 in the name you can match those sizes
                 Please respond in a json format.
                    
                 **Required Sizes**
                 {
                     question
                 }
                 
                 **Lookup Data**
                 ```json
                 {data}
                 ```
                 
                 **Context**
                 ```text
                 {context}
                 ```
                 
                 """
        )

        return {"Size-Data": response, "Declared-Base-Size": base_or_sample_size}

    def get_garment_piece_component_codes_lookup(self, question: str):
        """
        This is a generic lookup function for codes. If you are processing pieces on a particular body, you can check what those pieces might be called at Resonance company using this lookup function
        Garment pieces have Resonance standard naming conventions. For example the piece component `DRSBKYKEUNRT` is broken into the following parts
            DRS: product taxonomy
            BK: the piece part e.g. "The Back"
            YKE: the component name e.g. the Yoke piece
            UN RT: the orientation i.e. the under right piece
        You can search for specific components or search by component names and codes. Do not try to filter by garment names as this lookup covers all garments

        **Args**
          question: a question to trigger a search for garment piece name components e.g. load all or lookup specific size codes
        """
        # return self._piece_comp_store.run_search(context)
        return self._piece_comp_store.load().to_dicts()

    def get_rules_for_pattern_piece_symmetry_and_duplication(self, question: str):
        """
        Use this function to lookup rules to know how and why pattern pieces in a DXF might need to be mirrored or duplicated to make the full garment

        **Args**
          question: a question to trigger a search for garment piece name components e.g. load all or lookup specific size codes
        """
        F = """
          **Rules for duplication of pattern pieces**
        
          1. Often a pattern piece will exist only once and need to be duplicated for facing pieces or to make "double self" peces
          2. for example a back yoke could have one pattern piece (or set of pieces) for the TOP (TP) and require a second set for UNDER (UN)
          3. furthermore, the sketch might show that piece needs to be symmetric e.g. a back yoke but the pattern peices contains only the right side which needs to be unfolded or duplicated to complete the garment
          4. Generally certain pieces like doghouse sleeve plackets may require reinforcement with facing and under (UN) pieces
          
          Taking all of these together, for example we might have an non symmetric yoke pattern piece that we need to double once to complete the left and right and then double again to complete both the TOP and UNDER. This means a total of 4 pattern pieces (two of which are mirrored)
          
          When generating orientation codes we can have 0,2 or 4 characters. We start with Left/Right as LT/RT if relevant. Then we consider Top/Under TP/UN if relevant. Examples;
          - a left single piece sleeve panel would be SLPNLLT
          - a left under bake yoke piece for a yoke that has top and under would by BKYKELTUN
          to these we add the product taxonomy code e.g. dor a Dress it is DRS. Therefore the full piece code might look like DRSBKYKELTUN
          
          You can look up the piece codes that resonance uses for more information
          
        """

        return F

    def describe_visual_image(
        cls,
        url: str = None,
        question_relating_to_url_passed: str = "describe the image you see",
        **kwargs,
    ):
        """
        A url to an image such as a png, tiff or JPEG can be passed in and inspected
        A question can prompt to identify properties of the image
        When calling this function you should split the url out of the question and pass a suitable question based on the user question

        **Args**
            url: the uri to the image typically on s3://. Can be presigned or not
            question_relating_to_url_passed: the prompt to extract information from the image

        see: https://platform.openai.com/docs/guides/vision
        """

        # sometimes the llm i creative with the parameters. any url will do if we have none
        if url is None:
            for k, v in kwargs.items():
                if "uri" in k or "url" in k:
                    url = kwargs[k]
        assert (
            url != None
        ), "You must pass in a url parameter - check the technical notes for the correct s3 path to the image"
        url = (
            url
            if "AWSAccessKeyId" in url
            else res.connectors.load("s3").generate_presigned_url(url)
        )

        res.utils.logger.debug(f"{url=}, {question_relating_to_url_passed=}")

        response = openai.chat.completions.create(
            model="gpt-4-vision-preview",
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": question_relating_to_url_passed},
                        {
                            "type": "image_url",
                            "image_url": url,
                        },
                    ],
                }
            ],
            max_tokens=3000,
        )

        desc = response.choices[0].message.content

        return f"Here is the description of the image as asked: {desc}"

    def check_should_post_file(cls, function_response, channel, thread_ts=None):
        """
        Working out a flow - here the idea is that if functions return instructions to post files in we can just generically deal with this here.
        we hard coded the api function caller helper to still only return json rather than dealing with arb types
        the files are a special case where we save the files on s3 and then serve -> direct to bytes would be good to but this works for now
        """
        print(function_response, channel)
        try:
            # TODO: model this would a proper return type for typed behaviors
            if not isinstance(function_response, dict):
                # we can try removing fencing for some cases
                function_response = function_response.replace("```json", "").replace(
                    "```", ""
                )
                function_response = json.loads(function_response)
            files = function_response.get("files")
            # dont past the same file twice
            # files = [f for f in files if f not in cls._posted_files]
            if files and channel:
                res.utils.logger.debug(
                    f"Checking for files to post - {files} to channel {channel} "
                )

                cls._slack.post_s3_files(files, channel=channel, thread_ts=thread_ts)
                res.utils.logger.info(f"Sent files {files} to channel = {channel}")
                cls._posted_files += files
        except Exception as ex:
            res.utils.logger.warn(
                f"Problem checking files to post {ex} when parsing from {function_response}"
            )

    def __call__(cls, *args, **kwargs):
        return cls.run(*args, **kwargs)

    def run(
        cls,
        question: str,
        limit=10,
        use_data: dict = None,
        response_format={"type": "json_object"},
        channel_context=None,
        user_context=None,
        prompt=None,
    ) -> dict:
        """
        run the interpreter loop based on the PLAN

        **Args**
            question: question from user
            limit: the number of loops the interpreter can run for before giving up
            response_format: force response format (deprecate)
        """

        # store question for context
        cls._posted_files = []
        cls._track_single_call = []
        cls._question = question
        cls._messages = [
            {"role": "system", "content": prompt or cls.PLAN},
            {"role": "user", "content": question},
        ]

        cls._active_functions = cls._built_in_functions
        cls._active_function_callables = {
            f.name: f.function for f in cls._active_functions or []
        }

        res.utils.logger.debug(f"{cls._active_function_callables=}")
        tokens = 0
        for idx in range(limit):
            # functions can change if revise_function call is made
            functions_desc = [f.function_dict() for f in cls._active_functions or []]

            response = openai.chat.completions.create(
                model=DEFAULT_MODEL,
                messages=cls._messages,
                functions=functions_desc or None,
                response_format=response_format,
                function_call="auto" if functions_desc else None,
            )

            response_message = response.choices[0].message
            logger.debug(response_message)

            function_call = response_message.function_call

            if function_call:
                fn = cls._active_function_callables[function_call.name]
                args = function_call.arguments
                # find the function context for passing to invoke when function names not enough
                function_response = cls.invoke(fn, args)
                logger.debug(f"Response: {function_response}")
                cls._messages.append(
                    {
                        "role": "user",
                        "name": f"{str(function_call.name)}",
                        "content": json.dumps(
                            function_response, cls=NpEncoder, default=str
                        ),
                    }
                )

            MSG = len(json.dumps(cls._messages, cls=NpEncoder, default=str))
            logger.info(
                f"<<<<<<< Looping with messages at index {idx} with message count {len(cls._messages)} of total sizes {MSG} and token usage {response.usage} >>>>>>>>"
            )
            logger.debug(
                f"Functions loaded are {list(cls._active_function_callables.keys())}"
            )
            tokens += response.usage.total_tokens

            if response.choices[0].finish_reason == "stop":
                logger.info(f"Completed with total token usage {tokens}")

                # if function responded with file and channel, post file
                # we could do this from function response too but here just doing it at end
                cls.check_should_post_file(
                    response_message.content, channel=channel_context
                )

                break

        res.utils.logger.info(
            "---------------------------------------------------------------------------"
        )
        res.utils.logger.info(response_message.content)
        res.utils.logger.info(
            "---------------------------------------------------------------------------"
        )

        return response_message.content or "There were no data found"

    def ask(cls, question: str, model=None, response_format=None):
        """
        this is a direct request rather than the interpreter mode
        response_format={"type": "json_object"}
        """
        plan = f""" Answer the users question as asked  """

        logger.debug("asking...")
        messages = [
            {"role": "system", "content": plan},
            {"role": "user", "content": f"{question}"},
        ]

        response = openai.chat.completions.create(
            model=model or DEFAULT_MODEL,
            messages=messages,
            response_format=response_format,
        )

        # audit response, tokens etc.

        return response.choices[0].message.content


class Assimilator(BodyOnboarder):
    PLAN = """Your job is to compare multiple data sources in order to arrive at a single version of truth. 
        You should expect that all separated data sources are incomplete and have partial truths, inferences, missing data etc.
        You should expect some hints about low confidence in certain data and then you can instead turn to other data sources to fill in the missing pieces.
        For example, we may use a classifier to identify pieces in a DXF file but may not identify all pieces. Meanwhile we may have a PDF document of notes that describes what is expected.
        So for example we might (mis)identify a rectangular a DXF pieces as being a facing or ruffle piece when in fact it is the tab or placket piece mentioned in the notes. 
        - For rectangular pieces the dimensions are very important to observe
        - For non symmetric pieces (like back yokes) that we expect to be symmetric, we may need to "complete" the other half by duplicating a pattern pieces if we only have one and expect two for example
        Respond in a suitable JSON format
        """


class BodyPartsFinder5(BodyOnboarder):
    PLAN = """
      Your job is to list all pieces observed in any of the data; dxf pattern pieces, teh pack, sketches of the garment (front and back), etc.
      Seams and hems are note pieces. You should include them as pieces but you can reference them in piece notes
      if the pieces appears multiple times in the garment e.g. left and right or top and under you should mention this - instances are important
    
      **Response Format**
      ```json
         class PieceMention(BaseMode):
            piece_name: str
            notes: str
            
        class PieceMentionSet(BaseModel):
            pieces: List[PieceMention]
      ```
      
        Please list as may pieces as you can with additional notes as you observe them per piece
    """


class BodyPartsFinder4(BodyOnboarder):
    PLAN = """
      Your job is to scour the technical data for this body which is in the form of technical specifications and sketches. 
      Diagrams and sketches will depict the style of the garment from the front and back.
      Carefully observe all pieces mentioned and list them with the output format below.
      
      **Notes**
      1. Do not include seam and hems as pieces. These are not pieces but its useful to add notes about them to related pieces
      2. The DXF file should be consulted at the end since it has pattern pieces. You should observe which pattern pieces do or could related to each piece in the notes
    
      **Response Format**
      ```json
         class PieceMention(BaseMode):
            piece_name: str
            notes: str
            connected_to_pieces: List[str]
            has_top_and_under: bool
            has_left_and_right: bool
            is_symmetrical: bool
            fusing_notes: str
            approximate_area_percentage_of_piece: float
            
        class PieceMentionSet(BaseModel):
            pieces: List[PieceMention]
      ```
      
        Please list as may pieces as you can with additional notes as you observe them per piece
    """

    # add pattern pieces later and piece codes


class BodyPartsFinder3(BodyOnboarder):
    PLAN = """
      your job is to list possibly pieces mentioned in possibly contradictory sources. You must discover all pieces used to make the garment
      1. The best first place to check is the front and back sketch of the garment (the uri is provide in the data)
      2. next look at comments in general data about the garment where pieces are mentioned - sometimes internal or lining pieces will be mentioned that the sketch does not show and you can note that here
      3. next you can look at the DXF pattern pieces - these pieces are classified but the rectangular piece names are unreliable. In fact those rectangular pieces are likely to be another piece mentioned in the first two sources
      
      DXF pattern pieces can be used twice and in some rare cases some pieces mentioned might not have a pattern piece in the DXF file
      
      Response in the following JSON format (Pydantic)
      
      **Response Format**
      ```json
         class PieceMention(BaseMode):
            piece_name: str
            notes: str
            piece_counts_and_symmetry_notes: str 
            sources: str
            observation_confidence: float
            
        class PieceMentionSet(BaseModel):
            pieces: List[PieceMention]
      ```
      
        Please list as may pieces as you can
    """


class BodyPartsFinder2(BodyOnboarder):
    PLAN = """
    your job is to collect as much data about each piece the tech pack followed by the DXF pattern pieces. 
    You can and should lookup resonance piece naming conventions and strictly use those codes in your response re: codes.
    Make your comments as rich as possible so that we can refined the details in a second pass if necessary
    DO NOT trust rectangular piece names in DXF files unless the names are referenced elsewhere and assume that rectangular pieces fulfil the role of pieces mentioned elsewhere
    You the Pydantic model below for your Json response structure; here are some hints
    - non symmetric pieces may need to reuse pattern pieces to complete the shape
    - do not use symmetry orientations unless there is a need to discriminate multiple similar pieces
    - note when there is a need to have left and right pieces are top and under pieces since your job is to make a FULL list of all pieces required to make the garment. If you omit pieces it can lead to construction problems.
    
    ```Pydantic Model
        class GarmentPiece(BaseModel):
            #seams are usual described as operation type, a measurement in inches
            name: str
            description: str
            #e.g. Dress or Skirt
            product_code: str
            #list pieces adjacent to this piece if know or inferred
            connects_to_pieces: List[str] = []
            #what part of the body is it e.g. FT (Front) or SLV (Sleeve) or NK (Neck)
            part_code: str
            #what type of piece is it? use the official three letter Resonance code e.g. Pocket (PKT), Placket (PLK), or Panel (PNL)
            component_code: str
            #orientations are IMPORTANT to make sure we have a full manifest of pieces 
            # there can be two or more orientations in the full garment 
            # e.g. Sleeve left and right or Yoke Left and right with top and under of each for double yokes. 
            # The yoke must be symmetric and we can use two non-symmetric pattern pieces to make a whole
            orientation_codes: List[str] 
            observation_confidence: float
            evidence_for_piece: str
            #if known, add one or more pattern pieces used for the physical pieces
            #we can use the identified pattern piece - consider the area of the piece etc. to confirm it is a logical choice
            all_piece_indices_in_dxf_pattern_pieces: List[int]
            #based on normal garment standards, should be piece be used e.g. collars and cuffs normally are and there may be some comment on the notes
            fusing_comments: str
            seam_operation_comments: str
            button_count: int = 0
            
        class AllGarmentPieces:
            pieces: List[GarmentPiece]
            #pattern pieces can be duplicated therefore there may be a different number of physical pieces to pattern pieces
            number_of_pieces_in_dxf: int
            #the sketch can provide a good idea how many physical pieces there should be and this can be matched to the pattern pieces
            distinct_pieces_observed_in_sketch: str
            #the notes can mention pieces in short hand and you can compare with what is in sketch or pattern file
            pieces_mentioned_in_techpack: str
            #check your work - of there is a piece mentioned in one of the sources that are not listed in your result, you can note it here
            pieces_not_fully_identified: str
    ````
    ```
    
    Follow this strategy when listing all pieces that are mentioned
    1. Ask how are the front and back of the garment described in the sketch image - list all plackets, tabs, pockets and other accessory pieces that are mentioned 
    2. review the tech pack pdf notes in general and collect all notes about the pieces and sewing operations
    3. next check the DXF pattern pieces so you can corroborate pieces that appear there and in tech pack but DO NOT trust the rectangular piece names and instead infer which pieces in the notes could be the rectangular pieces
    4. Use Resonance convention for piece component naming
    """


class BodyPartsFinder(BodyOnboarder):
    PLAN = """
    garments can be one of a number of types e.g. dress, shirts, skirts, trousers/pants, one-piece jumpsuits. 
    - normally garments have two sleeves. the sleeves may have additional pieces attached like cuffs, plackets and tabs. you should list each doubled piece e.g. left sleeve and right sleeve 
    - the garment consists have the following regions; the sleeves area, the neck area, the bodice area and bottom area
    - the bodice is usually made up have a back panel and front panel. often the front panel is made up 2 pattern pieces that are joined together by plackets/buttons.
    - the collar may have an inner part called a collar stand.
    - front panels or pants front or back may have patch pockets.
    - seams/hems are not pieces but rather edges between or at end of pieces. for example the arm hole seam connects the bodice region to the sleeve region
    
    Resonance has a specific naming convention for all these pieces which you can look up. Do not guess the abbreviations as this can cause confusion.
    Note that the pieces have a product type like dress and then components and parts specified by their layout on the body and orientation.
    You should use all the information you have in pattern pieces and sketches to list the pieces that you see and what pattern piece corresponds to that piece. 
    - Use the following piece schema to return JSON data listing the pieces and take a look at the example answer
    - Use the evidence you have one by one to complete the picture (1) The tech pack pdf (2) Any sketches in the text pack (3) Dxf Pattern pieces (4) lookup resonance conventions and terminology
    
    ```Pydantic Model
        class GarmentPiece(BaseModel):
            #seams are usual described as operation type, a measurement in inches
            name: str
            description: str
            #e.g. Dress or Skirt
            product_code: str
            #list pieces adjacent to this piece if know or inferred
            connects_to_pieces: List[str] = []
            #what part of the body is it e.g. FT (Front) or SLV (Sleeve) or NK (Neck)
            part_code: str
            #what type of piece is it? use the official three letter Resonance code e.g. Pocket (PKT), Placket (PLK), or Panel (PNL)
            component_code: str
            #orientations are IMPORTANT to make sure we have a full manifest of pieces 
            # there can be two or more orientations in the full garment 
            # e.g. Sleeve left and right or Yoke Left and right with top and under of each for double yokes. 
            # The yoke must be symmetric and we can use two non-symmetric pattern pieces to make a whole
            orientation_codes: List[str] 
            observation_confidence: float
            evidence_for_piece: str
            #if known, add one or more pattern pieces used for the physical pieces
            #we can use the identified pattern piece - consider the area of the piece etc. to confirm it is a logical choice
            all_piece_indices_in_dxf_pattern_pieces: List[int]
            #based on normal garment standards, should be piece be used e.g. collars and cuffs normally are and there may be some comment on the notes
            fusing_comments: str
            
        class AllGarmentPieces:
            pieces: List[GarmentPiece]
            #pattern pieces can be duplicated therefore there may be a different number of physical pieces to pattern pieces
            number_of_pieces_in_dxf: int
            #the sketch can provide a good idea how many physical pieces there should be and this can be matched to the pattern pieces
            distinct_pieces_observed_in_sketch: str
            #the notes can mention pieces in short hand and you can compare with what is in sketch or pattern file
            pieces_mentioned_in_techpack: str
            #check your work - of there is a piece mentioned in one of the sources that are not listed in your result, you can note it here
            pieces_not_fully_identified: str
    ````
    ```
    
    Please list ALL the pieces that are mentioned in one of these sources along with your confidence. Its better to suggest more pieces with low confidence than not mention pieces
    
    """


class BodyPieceValidator(BodyOnboarder):
    PLAN = """Your job is to review the pieces proposed by separate attempts. You should try to produce a refined piece description by combining the best answers
                Answer quality depends on the following
                1. The pieces listed are all referenced in the tech pack implicitly or explicitly. What we name in the DXF file is inferred and unreliably for rectangular pieces.
                2. The piece components and part codes should all exist in Resonance's pieces abbreviations lookup and all orientations required to make the garment should be included
                3. The piece index for the DXF pattern shapes should all be paired with pieces mentioned elsewhere. Ideally the names in DXF should corroborate names used elsewhere but sometimes the DXF names cannot be trusted
                
            Use the same format below to compile your answer
            
                ```Pydantic Model
                    class GarmentPiece(BaseModel):
                        #seams are usual described as operation type, a measurement in inches
                        name: str
                        description: str
                        #e.g. Dress or Skirt
                        product_code: str
                        #list pieces adjacent to this piece if know or inferred
                        connects_to_pieces: List[str] = []
                        #what part of the body is it e.g. FT (Front) or SLV (Sleeve) or NK (Neck)
                        part_code: str
                        #what type of piece is it e.g. Pocket, Placket, or Panel
                        component_code: str
                        #orientations are IMPORTANT to make sure we have a full manifest of pieces 
                        # there can be two or more orientations in the full garment 
                        # e.g. Sleeve left and right or Yoke Left and right with top and under of each for double yokes. 
                        # The yoke must be symmetric and we can use two non-symmetric pattern pieces to make a whole
                        orientation_codes: List[str] 
                        observation_confidence: float
                        evidence_for_piece: str
                        #if known, add one or more pattern pieces used for the physical pieces
                        #we can use the identified pattern piece - consider the area of the piece etc. to confirm it is a logical choice
                        all_piece_indices_in_dxf_pattern_pieces: List[int]
                        #based on normal garment standards, should be piece be used e.g. collars and cuffs normally are and there may be some comment on the notes
                        fusing_comments: str
                        
                    class AllGarmentPieces:
                        pieces: List[GarmentPiece]
                        #pattern pieces can be duplicated therefore there may be a different number of physical pieces to pattern pieces
                        number_of_pieces_in_dxf: int
                        #the sketch can provide a good idea how many physical pieces there should be and this can be matched to the pattern pieces
                        distinct_pieces_observed_in_sketch: str
                        #the notes can mention pieces in short hand and you can compare with what is in sketch or pattern file
                        pieces_mentioned_in_techpack: str
                        #check your work - of there is a piece mentioned in one of the sources that are not listed in your result, you can note it here
                        pieces_not_fully_identified: str
                ````
        """


from res.learn.agents.BodyOnboarder import *

# when happy, now we update the bodies table and flow - designation a body code (test of whatever)
# call update body on the adjacent statuc meta one table
# then create another process that, on validating the input in said staging, merges -> creating bodies or whatever and body one ready requests


class PieceRefiner(BodyOnboarder):
    PLAN = """
   To name pieces you need to understand garment structure. There are the following major areas. Each area has a code. You can look up the codes using a function before consider the following recipe;
   **Major Parts - parts are designated with a code pf length 2**
   1. The Front Bodice (FT): These are made of panels which may be a single panel (PNL) piece or split into parts e.g left (LF) and right (RT) (FTPNLLF/FTPNLRT)
   2. The Back Bodice (BK)
   3. The Neck (NK)
   4. The Sleeves (SL)
   5. Bottoms - see lookup of parts
   
   ** Major Components - parts are designated with a code of length three - we combine the area and parts to make make codes of length 5**
   - As well as panel (PNL), the front or back bodice area can have parts like the Yoke (YKE). If the yoke is in the front part of the bodice it is FtYKE or if it is in the back it is BKYKE
   - the neck can have collars (CLR) with collar stands (CLS) and bindings (BDG) for example. As they are in the neck region they have names like NKCLR, NKCLS NKBDG
   - the sleeves which are typically left and right can have panels (PNL), cuffs (CUF), plackets (PLK) and various other accessory pieces
   - when making three part codes you can ignore adjectives e.g. a Doghouse placket is just a placket, and a patch pocket is just a pocket, we dont need the qualifier
   
   ** Orientations - when a component is made up multiple sub components we designate an orientation to each e.g. top/under left/right etc. orientation code can be length 0,2 4 depending on the component complexity**
   - for example we can have neck collar pieces that are fused resulting in top and under pieces e.g. NKCLRTP and NKCLSUN
   - we can have left and right pieces like left sleeve panels (SLPNLLT/SLPNLRT) or back yoke made of two parts (BKYKELF/BKYKERT)
   - we can also have more complexity where we have e.g. 4-element yokes for left/right/top/under i.e. BKYKELFTP / BKYKELFUN / BKYKERTTP /BKYKERTUN
   - when looking at the data, any mention of facings, doubles and similar can indicate a top/under (TP/UN) pairings e.g. for collars, yokes, plackets etc. In this case you should add all the orientations
   
   ** About Accessory pieces**
   Accessory pieces should be named based on what part of the garment they are on e.g. a sleeve placket on the left is SLVPLKLF
   
   ** Garment categories **
   -- garment categories have codes like Dress (DRS), Shirt (SHT etc)
   
   ** Advice **
   - When given piece lists, you can construct the piece codes using this knowledge. Note these are just examples so you will need to lookup specific piece codes given garment names. 
   - You should never use a part, component, orientation code that does not exist in the lookup even if they are given in examples above
   - You should always respect the length of the codes e.g. Part:(2), Component:(3), Orientation: (2 or 4 or 0)
   - You can group pieces by their part/component and this list all orientations that exist. for example for a collar you can add the top and under in the list of orientation codes or if pieces have left and rught pieces add both orientations
   - for Self or Facing pieces you can use Top (TP) and Under (UN) respectively
   
   You Job is to take the input piece names and return garment names in the following format
   
   **Response Model**
   class PiecesModel(BaseModel):
    #you can group pieces with the same base of PART and COMPONENT and then list each of their orientations
    input_names_in_group: List[str]
    #the part code of the garment (length 2)
    part_code: str
    #the component code of the garment (length 3)
    component_code:str
    #if the piece is complex i.e. is made of multiple elements, list the specific orientation codes for all the elements otherwise leave the orientations empty.
    #these codes if they exist will be 2 or 4 characters  
    all_orientation_code_mapped_to_dxf_indices: dict
    notes: str
    
   class Garment(BaseModel):
    #if you dont know the garment details omit
    garment_name: str
    #if you dont know the garment details omit
    garment_category: str
    #if you dont know the garment details omit
    garment_category_code: str
    pieces: List[PiecesModel]
    have_i_checked_all_code_lengths: bool
    #pecent
    confidence: float
    comments: str

    Respond in the JSON format given by the model 
   
"""

    def __init__(self):
        self._sizes_store = ColumnarDataStore(ResonanceSizesModel)
        self._piece_comp_store = ColumnarDataStore(PieceComponentsModel)

        self._built_in_functions = [
            # describe_function(self.search_ingested_techpack_content_for_body),
            # describe_function(self.get_resonance_general_size_codes),
            # describe_function(
            #     self.get_general_construction_info_for_rectangular_pieces
            # ),
            describe_function(self.get_garment_piece_component_codes_lookup),
            describe_function(self.describe_visual_image),
            # describe_function(self.get_dxf_pattern_pieces_description),
        ]

    def refine_piece_data(self, data):
        P = f"""
        The following pieces were identified in the garment - please determine their Resonance piece codes:

        **Piece Names In Garment**
        ```json
        {json.dumps(data)}
        ```

        """
        return self(P)


"""
Example outputs: interesting as its parsed from the pdf docs + other stuff

{'name': 'THE MAXI DRESS',
 'category': 'ladies dress',
 'gender': 'Women',
 'size_scale': 'US',
 'base_size': '2',
 'supported_materials': ['SELF', 'FUSIBLE', 'BUTTONS', 'THREAD'],
 'pieces': [{'name': 'Back Yoke',
   'piece_component_code': ['BK', 'YKE'],
   'symmetries': ['CB'],
   'seam_allowance_inches': 3.75,
   'seam_operations': [{'name': 'Edgestitch',
     'needles': 'Single',
     'size_inches': 0.0625},
    {'name': 'Flat Felled Seams', 'needles': 'Double', 'size_inches': 0.375},
    {'name': 'Box Pleat', 'needles': 'Single', 'size_inches': 0.25},
    {'name': 'Topstitch', 'needles': 'Single', 'size_inches': 0.25}],
   'piece_description': 'Back Yoke is double self with a center back seam on yoke facing and yoke with box pleat centered at back yoke seam.'},
  {'name': 'Front Placket',
   'piece_component_code': ['FT', 'PLK'],
   'symmetries': ['CF'],
   'seam_operations': [{'name': 'Topstitch',
     'needles': 'Double',
     'size_inches': 0.25}],
   'piece_description': '1/4" single needle topstitch at center front placket.'},
  {'name': 'Collar Stand',
   'piece_component_code': ['NK', 'CLS'],
   'seam_operations': [{'name': 'Edgestitch',
     'needles': 'Single',
     'size_inches': 0.0625}],
   'piece_description': '1 button at collar stand.'},
  {'name': 'Sleeve Cuff',
   'piece_component_code': ['SL', 'Cuff'],
   'seam_operations': [{'name': 'Edgestitch',
     'needles': 'Single',
     'size_inches': 0.0625},
    {'name': 'Topstitch', 'needles': 'Single', 'size_inches': 0.25}],
   'piece_description': '1 button at each sleeve cuff and sleeve placket. One button at each sleeve tab.'},
  {'name': 'Patch Pocket',
   'piece_component_code': ['PK'],
   'symmetries': ['LF'],
   'seam_operations': [{'name': 'Edgestitch',
     'needles': 'Single',
     'size_inches': 0.0625},
    {'name': 'Topstitch', 'needles': 'Single', 'size_inches': 0.0625}],
   'button_count': 1,
   'button_size': 'Small',
   'piece_description': 'Patch pocket at wearer\'s left chest with 3/8" turnback then 1" turnback and topstitch at opening. 1/16" edgestitch around pocket with triangle reinforcement at pocket corners.'},
  {'name': 'Doghouse Placket',
   'piece_component_code': ['SL', 'PLK'],
   'seam_operations': [{'name': 'Edgestitch',
     'needles': 'Single',
     'size_inches': 0.0625},
    {'name': 'Bias Binding', 'needles': 'Single', 'size_inches': 0.25}],
   'piece_description': 'Doghouse placket on sleeve with 1/16" edgestitch. 1/4" bias binding to finish sleeve slit.'},
  {'name': 'Sleeve Tab',
   'piece_component_code': ['SL', 'TAB'],
   'seam_operations': [{'name': 'Edgestitch',
     'needles': 'Single',
     'size_inches': 0.0625}],
   'piece_description': '1 button at each sleeve tab.'},
  {'name': 'Front',
   'piece_component_code': ['FT', 'BOD'],
   'seam_operations': [{'name': 'Edgestitch',
     'needles': 'Single',
     'size_inches': 0.0625},
    {'name': 'Flat Felled Seams', 'needles': 'Double', 'size_inches': 0.375},
    {'name': 'Hemming', 'needles': 'Double', 'size_inches': 0.25}],
   'piece_description': 'Front dress is unlined, with flat felled seams at sleeve seams, armholes, and side seams. Hem is 1/4" double turnback and topstitch.'},
  {'name': 'Back',
   'piece_component_code': ['BK', 'BOD'],
   'seam_operations': [{'name': 'Flat Felled Seams',
     'needles': 'Double',
     'size_inches': 0.375},
    {'name': 'Hemming', 'needles': 'Double', 'size_inches': 0.25}],
   'piece_description': 'Back dress is unlined, with flat felled seams at sleeve seams, armholes, and side seams. Hem is 1/4" double turnback and topstitch.'}],
 'possible_piece_names': ['Front',
  'Back',
  'Collar Stand',
  'Sleeve Cuff',
  'Patch Pocket',
  'Doghouse Placket',
  'Sleeve Tab',
  'Hem'],
 'sketch_image_uri': 's3://res-data-platform/samples/images/techpack1.pdf_page_scan_3.png',
 'pattern_pieces_layout_image_uri': 's3://res-data-platform/samples/images/pattern_pieces_resFC928CF126.png'}
"""
