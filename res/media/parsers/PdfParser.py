import PyPDF2
import fitz
from PIL import Image, ImageOps
import io
from pdf2image import convert_from_path
import typing
import res

from res.learn.agents.builder.utils import (
    ask,
    describe_visual_image,
    describe_and_save_image,
)
from tenacity import retry, wait_fixed, stop_after_attempt


class DefaultTechPackPageParser:
    """
    This is a generic page pager for parsing tech packs in an opionated way
    """

    IMAGE_PROMPT = """
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

    # itemization seems like a subtle and important thing
    STRUCTURED_PAGE_PROMPT = """
        - Extract all the tabular data into a valid yaml format. Be careful to parse ALL measurements (assumed in inches) and table values and omit any numeric indexes from lists.
        - When extracting measurements capture the measurement name, value in inches and any comments.
        - If there is no interesting tabular data return a simple message in yaml format to say so.
        - Please groups things that are itemized separately from things that are not.
        - "Itemized" means there is a monotonic sequential counter as the first column in the table.
        - Please provide a clean indication of any sequential numeric index in all cases especially if a similarly named item appears twice in a list.
        - If the itemization does not start at 1 or 0 provide a comment that it may link to another list.
      
        """

    def __init__(
        self,
        filename,
        index,
        page_text,
        page_scan,
        page_images,
        key=None,
        number_pages=None,
        **kwargs,
    ):
        self.filename = filename
        self.page_number = index
        self.page_text = page_text
        self.page_scan = page_scan
        self.page_images = page_images
        self._scan_description = None
        self._sketch_descriptions = []
        self._data = None

    @retry(wait=wait_fixed(5), stop=stop_after_attempt(3), reraise=True)
    def parse(cls, out_key):
        from res.observability.dataops import parse_fenced_code_blocks
        import yaml

        im = cls.page_scan
        res.utils.logger.debug(
            f"We have {len(cls.page_images)} sub images in this page"
        )

        sketch_texts = []
        page_images = cls.page_images
        if not len(page_images):
            page_images = [im]
        for j, sketch_image in enumerate(page_images):
            s = describe_and_save_image(
                sketch_image,
                prompt=cls.IMAGE_PROMPT,
                uri=f"{out_key}/page{cls.page_number}/image{j}.png",
            )
            sketch_texts.append(s)
            res.utils.logger.debug(s)

        full_prompt = f"""You are parsing a file called {cls.filename} - and this is page {cls.page_number} . 
        Assume a PDF is a Tech Pack or Cutter's Must and the file name may tell you. 
        Note the tech pack is more general and the Cutter's Must may provide extra context about the pieces required in the pattern.
        
        {cls.STRUCTURED_PAGE_PROMPT}
        """
        d = describe_and_save_image(
            im,
            prompt=full_prompt,
            uri=f"{out_key}/page-scans/image{cls.page_number}.png",
        )
        a = parse_fenced_code_blocks(d["description"], select_type="yaml")
        if a:
            a = yaml.safe_load(a[0])
        else:
            a = {
                "error": "there was an error loading this part of the document - there may be no tabular data of interest"
            }
        a["image_uri"] = d["uri"]
        # taken from above, the sketch description if relevant
        a["page_image_description"] = sketch_texts

        cls._data = a

        return a


class PdfParser:
    def __init__(
        self,
        uri,
        min_size=(300, 300),
        page_parser_cls=DefaultTechPackPageParser,
    ):
        self._uri = uri

        res.utils.logger.info(f"Opening {uri}...")
        s3 = res.connectors.load("s3")

        pdf_path = (
            io.BytesIO(open(uri, "rb").read())
            if not "s3://" in uri
            else s3.as_stream(uri)
        )

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
                    # if transform and "-" in str(transform[2]):
                    #     image = ImageOps.flip(image)
                    image_list.append(image)

                self._page_images.append(image_list)

        # these are the full page scans of the page
        if "s3://" in uri:
            self._image_scans = s3.process_using_tempfile(uri, fn=convert_from_path)
        else:
            self._image_scans = convert_from_path(uri)

        assert len(self._image_scans) == len(
            self._page_images
        ), "we expect the same number of pages as page data"
        assert len(self._parsed_text) == len(
            self._page_images
        ), "we expect the same number of pages as page data"

        self._page_parsers = []
        for i in range(self._num_pages):
            self._page_parsers.append(
                page_parser_cls(
                    filename=uri,
                    index=i,
                    page_text=self._parsed_text[i],
                    page_scan=self._image_scans[i],
                    page_images=self._page_images[i],
                )
            )

    def __getitem__(self, key):
        return self._page_parsers[key]

    def parse(self, out_key, prompt=None, from_cache=False, num_workers=4):
        """
        we read the document in parallel - if the prompt is passed we could guide to some specific information but generally we have a standard prompt for garment development
        """
        s3 = res.connectors.load("s3")
        path = f"{out_key}/reader_cached.json"

        res.utils.logger.info(
            f"Reading {self._num_pages} pages in total using {num_workers} workers..."
        )
        if from_cache and s3.exists(path):
            return s3.read(path)

        import concurrent.futures
        from functools import partial

        def f(i, uri, key):
            tp = PdfParser(uri)
            try:
                return tp[i].parse(out_key=f"{key}/{i}")
            except Exception as ex:
                # raise  # While testing at least
                return {"error": f"failed to parse page {i} - {ex}"}

        f = partial(f, uri=self._uri, key=out_key)

        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            future_results = [executor.submit(f, i) for i in range(self._num_pages)]
            results = [
                future.result()
                for future in concurrent.futures.as_completed(future_results)
            ]

        res.connectors.load("s3").write(path, {"data": results})

        return results
