import res
import res
import json
from res.learn.agents.builder.utils import ask, describe_visual_image

ROOT = f"s3://res-data-platform/meta-one-input/bodies"

# the cutters must currently has a particular format - we can become smarter about matching prompts to documents
# for now may do a hard coded thing based on file name
CUTTERS_MUST_PROMPT = """Here are a few pages of a Cutters Must used for garment construction. 
       It provides an itemized list of pieces which are numbered as Item No.
       It also contains some notes about which of those numbered pieces are fused.
       In addition there are general construction notes about trims and finishing.
       Please list each piece that is mentioned and state if it is fused or not. 
       Provide general notes about trims and finishing.
       Provide also the name and sizes of the garment.
       
       Respond in a json format.
"""


class BodyIntake:
    def __init__(self, body_code, body_version=0, input_dir=None, output_dir=None):
        body_code = body_code.upper().replace("_", "-")
        if "-" not in body_code:
            body_code = f"{body_code[:2]}-{body_code[2:]}"

        input_dir = input_dir or f"{ROOT}/{body_code}/v{body_version}/input"
        output_dir = output_dir or f"{ROOT}/{body_code}/v{body_version}/output"
        self._input_dir = input_dir
        self._output_dir = output_dir
        self._techpack_data = {}
        self._dxf_data = {}
        self._feedback_data = {}
        # temp
        self._piece_lu = res.connectors.load("s3").read(
            "s3://res-data-platform/samples/piece-comp.json"
        )

    def _read_for_json(self, key):
        s3 = res.connectors.load("s3")
        return {
            uri: s3.read(uri)
            for uri in s3.ls(self._output_dir)
            if f"/{key}/" in uri and ".json" in uri
        }

    @staticmethod
    def list_bodies():
        """
        we should store a metadata somewhere for this
        """

        def safe_split(s):
            s = s.split("/")
            if len(s) >= 5:
                return s[5]

        files = list(res.connectors.load("s3").ls(ROOT))
        bodies = set([safe_split(f) for f in files])
        return [b for b in bodies if b]

    @property
    def tech_pack_output(self):
        s3 = res.connectors.load("s3")
        if len(self._techpack_data) > 0:
            return self._techpack_data
        self._techpack_data = self._read_for_json("techpack")
        return self._techpack_data

    @property
    def feedback_output(self):
        if len(self._feedback_data) > 0:
            return self._feedback_data
        self._feedback_data = self._read_for_json("feedback-images")
        return self._feedback_data

    @property
    def dxf_output(self):
        if len(self._dxf_data) > 0:
            return self._dxf_data
        self._feedback_dxf_data_data = self._read_for_json("dxf")
        return self._dxf_data

    def build(self):
        """
        build image and
        """

        from res.learn.agents.builder.utils import (
            describe_and_save_image,
        )
        from res.media.parsers.PdfParser import PdfParser, DefaultTechPackPageParser

        s3 = res.connectors.load("s3")
        for file in s3.ls(self._input_dir):
            res.utils.logger.info(f"<<<<<<<<<< {file} >>>>>>>>>>")

            out_file = file.replace("/input/", "/output/").split(".")[0]
            ext = file.lower().split(".")[-1]

            if ".pdf" in file.lower():
                PdfParser(file).parse(out_file)
            if ext in ["jpeg", "png", "jpg", "tiff"]:
                # if feedback in the uri maybe adapt the prompt

                # some rules on file names maybe to switch prompts e.g. cutters must
                # this is a bit lame but we dont know how robust the auto stuff is
                prompt = DefaultTechPackPageParser.IMAGE_PROMPT
                if "cutter" in file.lower() and "must" in file.lower():
                    prompt = CUTTERS_MUST_PROMPT
                d = describe_and_save_image(
                    s3.read_image(file),
                    prompt=prompt,
                    uri=f"{out_file}.png",
                )
                s3.write(f"{out_file}.json", d)
            if "/dxf/" in file:
                res.utils.logger.info("implement dxf stuff")
        # describe pieces and dump
        d = self.describe_pieces(save=True)

        return d

    def inspect_techpack(self, question: str, as_json=False):
        data = self.tech_pack_output
        P = f"""You are an agent that helps to understand how to make a garment to high quality from tech pack inputs.
        The tech pack documents are normally PDF but we may have additional image content which will be described.
        The tech pack provides a general overview and the cutters must if available is the best reference to enumerate the actual pieces needed for the garment
        If the part name suffix is not given use "Panel" as a default otherwise use part names as given
        You should be careful when asked for quantities not to double count pieces needed to make the garment.
        You should observe piece details such as location, if they are paired left and right and top and under etc to understand how the pieces are put together
        
        ________
        
        Using the data below answer the user's question given below.
        
        **Data**
        ```
        {json.dumps(data)}
        ```
        
        **Question**
        ```text
        {
            question
        }
        ```
        """

        d = ask(P, as_json=as_json)

        if as_json:
            return json.loads(d)

        return d

    def list_pieces(self):
        """
        focus here is on making a complete list with correct quantities
        """
        d = self.inspect_techpack(
            """Please compile a list of mentioned pieces and the quantities of fabric pieces that need to be cut saying if its fused or not. 
            If the piece is fused or block fused, you do not need to add an extra quantity as this is just a boolean indicator saying that the piece requires fusing treatment.
            Do not omit any fabric pieces mentioned in the notes.
            **WARNING - HOW TO COUNT PIECES**
            -  for each piece mentioned state how many [PIECE] pieces need to be cut as they are listed in the manifest - do not assume a mistake in the listings because they are accurate!
            -  provide evidence for the quantity you provide since the piece could be referenced in multiple places (all of which you should carefully check)
            """,
            as_json=True,
        )
        return d

    def describe_garment(self):
        return self.inspect_techpack(
            "please describe the front and back sketch of the garment with as much detail about pieces, body regions and accessories as possible"
        )

    def describe_pieces(self, from_cache=False, save=True):
        """
        returns a list of pieces - this is done in two hops so we can make sure we find existing and quantity of pieces and then we
        confirm the names of pieces in regions makes sense
        """
        s3 = res.connectors.load("s3")

        uri = f"{self._output_dir}/described_pieces.json"

        if from_cache and s3.exists(uri):
            return s3.read(uri)

        pcs = self.list_pieces()
        res.utils.logger.debug(f"describing {pcs}...")

        P = f"""
           use these rules to check the piece names;
           - the pieces should say which region on the wearer the piece lies i.e. Neck, Sleeve, Front, Back. Bodice and Bottom regions can be omitted from the name in many cases unless their is room for ambiguity 
           - the piece should be qualified if relevant for example if there are two pieces then they may come in left/right pairs or top/under pairs which should be stated
           - state if the piece is fused or not
           - the piece should say WHERE and WHAT function the component has e.g. is it a pocket/placket/binding/tab/cuff - if no function is can be inferred and only the location information is given, default to 'Panel'
           - the pieces should not include hems, seams or trims like buttons
           - you should provide the original name and be sure to list distinct pieces (never omit qualified like left/right/top/under) etc from the piece name!!
           - IMPORTANT: you should confirm the quantity of each piece by checking the pieces that are enumerated in the notes
             - DO NOT double count across _different_ sources 
             - but DO count each mention in the same source
           
           Use the data below to list the correct piece descriptions
           
           **Data**
            ```json
             {json.dumps(pcs)}
            ```
            """

        d = self.inspect_techpack(
            P,
            as_json=True,
        )

        if save:
            # this is the parsed out two hop piece description in prep for uploading to airtable
            s3.save(uri, d)
        return d

    def evaluate_piece_descriptions(self, desc=None):
        """
        evaluate the pieces that we have described against the tech pack
        """
        desc = desc or self.describe_pieces(from_cache=True)
        P = f"""We think there are this many pieces in the garment. 

            ```json
            {json.dumps(desc)}
            ```
            
            Please comment on evidence for and against the quantities given 
            
            Notes
            - list omitted pieces
            - Any data indicating if a piece is fused is a boolean and does not suggest the need to add extra pieces.
            - You can trust the original source data as accurate as you evaluate our piece counts
            - state you confidence in the original answer for each piece
            - when looking at itemized data sets be sure to complete the set which may be split over multiple pages
        """

        a = self.inspect_techpack(P, as_json=True)
        return a

    def build_resonance_piece_names(self, data=None, common_names_only=False):
        """
        Given names, determine the resonance names
        """
        s3 = res.connectors.load("s3")
        uri = f"{self._output_dir}/resonance_piece_names.json"
        data = data or self.describe_pieces(from_cache=True)
        res.utils.logger.info(f"determining piece names for {data}...")

        P = f"""Please use the resonance codes store to name the pieces in the data below. The response format can be used as a guide.
        
        
        **Pieces Data**
        ```json
        {json.dumps(data)}
        ```

        **Resonance piece codes**
        ```
        {json.dumps(self._piece_lu)}
        ```        
        
        **Response Format**
        ```
        class PieceInfo(BaseModel):
            input_name: str
            input_piece_location_or_region: str
            #use the input quantity as a hint to see if we need to double pieces e.g. into top and under pairs in the orientation codes. The same part should only appear once in our list
            qty: int 
            #if known map the garment category to a 3 letter code - leave this blank if you are not told in the pieces data
            product_taxonomy_code: str
            #part component (2 letters) - Neck (NK), Sleeve (SL), Front (FT), Back (BK) etc. See the data for valid mappings
            part_code: str
            #component code (3 letters) - Panel (PNL), Cuff (CF), Placket (PLK) etc. See the data for valid mappings
            component_code: str
            #orientation codes can be blank or 2 or 4 letters. Start with Left/Right LF/RT and then Top/Under TP/UN or other mappings. Unless there are more than 4 similar parts, map other synonyms to these ones if relevant
            #if the data suggest that there are 2 fabric pieces for a part, then you might assume Top and Under pieces unless e.g. all pieces in sleeve regions come in left and right pairs or if the data refer to left and/or right
            orientation_codes: List[str]
            #piece type suffix. If the piece is self we use -S or if its Block-Fused or Fused we use -BF. Lining pieces are -LN if given in the data.  
            #enum -S | -BF | -LN | -C
            piece_type: str 
      
            
        class Pieces(BaseModel):
            #if the input has multiples, use the advice in the notes to infer the pairs of orientations so that each piece appears on its own in the list
            pieces: List[PieceInfo]
            notes: str
      
          ```
        
        ** Orientation Guidelines
        1. Any piece in a Sleeve region which means its part code is `SL` then  it normally comes in left an right orientation pairs so you should set the orientation to ['LF', 'RT']
        2. Front plackets are usually single pieces
        3. Back yokes which can be left and right pairs may appear as a single or double piece. If there is a double left right we need Left Top, Left Under, Right Top, Right Under
        4. If we have a piece in the list with the same name that has an under, we should also have a top and versa versa
        5. If there is not an obvious left and right pairing but a piece claims to come with 2 quantity, then you might assume a top and under pair e.g. for collar stands
        
        """

        d = json.loads(ask(P, as_json=True))
        s3.write(uri, d)
        return d

    def build_resonance_size_scale(self):
        pass

    def build_slack_index(self, channels):
        """
        Look into our saved stores and read all data related to this body
        create records with content and person references and save in a database
        """
        pass

    def as_agent(self, extra_functions=None):
        """
        create a slack aware agent that answers questions about the body and version
        we can create a wrapper function for use in a more general agent that calls this for body and version
        but we can use this one for testing on specific bodies
        """
        pass
