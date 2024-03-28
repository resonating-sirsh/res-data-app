"""

bare bones parser - opportunity to build prompting into this e.g. the image should be parsed in different ways
might make this a class and create strategies
"""


import res
from glob import glob
from tqdm import tqdm
from PIL import Image


class ContentParser:
    PROMPT_IMAGE_EXTRACTION = f"""Please describe the image content in detail. Images are usually figures in documents that illustrate strategies and methodologies
                                so it is important to reproduce the text, messaging and visual guides of the image. 
                                In the case where the image is simply a logo or an image of a person and is unlikely to convey and rich information supporting the document,
                                you can briefly say its a "logo" or an "image of a person" without explanation.
                                You can assume the context for the rest of the document is known to the user"""

    def __init__(self, prompt_image_extraction=None, min_image_parsing_size=(200, 200)):
        from res.learn.agents import InterpreterAgent

        self._agent = InterpreterAgent()

        self._prompt_image_extraction = (
            prompt_image_extraction or ContentParser.PROMPT_IMAGE_EXTRACTION
        )
        self._min_image_parsing_size = min_image_parsing_size

    def describe_garment_info_image(self, image: Image, name: str = None):
        """
        considering for special flows and contexts we know what we are looking for. maybe.
        """
        # example prompt
        prompt = f"""
        Please extract data from the images. Each image conveys different information and collectively they describe a garment construction.
        I would like to know the garment name,a list of all pieces, the size scale. 
        For each piece i would like to know how many buttons it has. It may say things in a table called "buttons" about there being `n @ piece` which means there are n buttons on a particular piece.
        Please also mention all hems and seams along with details of any stitching operations.
        
        """

        return self.describe_image(image, prompt=prompt)

    def describe_image(self, image: Image, prompt: str, name: str = None):
        """
        give a numpy or PIL image or a uri to an image - describe it

        by writing the image to s3 we can ask the agent to describe it

        name: is optional for a handle to the image otherwise everyone writes to the same place which is fine for testing
        prompt: what are we trying to extract
        """

        s3 = res.connectors.load("s3")
        name = name or "staged"
        uri = f"s3://res-data-platform/samples/images/{name}.png"
        s3.write(uri, image)

        res.utils.logger.debug(f"describing {uri}")
        desc = self._agent.describe_visual_image(
            s3.generate_presigned_url(uri), question=prompt
        )

        return {"uri": uri, "description": desc}

    def add_to_doc_store(self, records, name, namespace):
        """ """
        from res.observability.entity import AbstractVectorStoreEntry
        from res.observability.io import VectorDataStore

        model = AbstractVectorStoreEntry.create_model(name=name, namespace=namespace)
        store = VectorDataStore(model, create_if_not_found=True)
        store.add(records)
        return store

    def add_to_l2_doc_store(
        self, records, name: str = "l2_docs", namespace: str = "executive"
    ):
        """
        a special case we started with and an example of how we update the store
        """
        return self.add_to_doc_store(records, name, namespace)

    def add_doc_to_stores(self, uri: str, name: str, namespace: str, model=None):
        """
        For any content we can add it to a vector store
        It assumes its text or text of an image for now but there are all sorts of things we can do the embed content
        """
        # determine provider pdf, docx, images

        from res.observability.entity import AbstractVectorStoreEntry

        ext = uri.lower().split(".")[-1]

        provider = (
            self.extract_images_and_text_from_pdf_doc
            if ext == "pdf"
            else self.extract_images_and_text_from_docx_doc
        )

        data = provider(uri, model=model or AbstractVectorStoreEntry)

        return self.add_doc_to_stores(data, name=name, namespace=namespace)

    def extract_images_and_text_from_docx_doc(self, uri: str, model=None):
        from docx_parser import DocumentParser

        doc = DocumentParser(uri)
        text = ""
        for _type, item in doc.parse():
            if isinstance(item, list):
                pass  # for now
            #             for i in item:
            #                 s += item.get('text')
            else:
                text += item.get("text")

        name = uri.split("/")[-1]
        page_num = 1

        d = {
            "uri": uri,
            "doc_id": res.utils.res_hash(uri.encode()),
            "name": f"{name}_page_{page_num}",
            "text_node_type": "DOCX_FRAGMENT",
            "page": page_num,
            "text": text,
        }

        yield d if not model else model(**d)

    def get_image_description(self, uri: str):
        """
        give the content parsers prompt, extract an image description
        its assumed in this mode we are linking the content somehow by uris
        """
        s3 = res.connectors.load("s3")

        res.utils.logger.debug(f"describing image {uri}")
        uri = s3.generate_presigned_url(uri)

        return self._agent.describe_visual_image(
            url=uri, question=self._prompt_image_extraction
        )

    def get_pdf_pages_as_images(self, uri):
        from pdf2image import convert_from_path

        pages = convert_from_path(uri)
        return pages

    def extract_images_and_text_from_pdf_doc(
        self, pdf_path: str, model=None, get_pages_as_images=True
    ):
        """
        TODO refactor for s3 or local file i.e. test all code paths / roughly its done
        Parse PDF files
        """
        import PyPDF2
        import fitz
        from PIL import Image
        import io
        from res.learn.agents import InterpreterAgent

        s3 = res.connectors.load("s3")
        name = pdf_path.split("/")[-1]
        uri = pdf_path

        # todo: io.BytesIO(input_zip.read(name))
        pdf_path = (
            io.BytesIO(open(pdf_path, "rb").read())
            if not "s3://" in pdf_path
            else s3.as_stream(pdf_path)
        )

        # Extract tables from a PDF using PyPDF2
        def extract_text(pdf_path):
            # with open(pdf_path, "rb") as pdf_file:
            pdf_reader = PyPDF2.PdfReader(stream=pdf_path)

            for page_num in range(len(pdf_reader.pages)):
                page = pdf_reader.pages[page_num]
                text = page.extract_text()  # Extract text from the page

                # TODO smarter pdf text processing e.g remove headers and preamble etc from the text

                d = {
                    "uri": uri,
                    "doc_id": name,
                    "name": f"{name}_page_{page_num}",
                    "text_node_type": "PDF_FRAGMENT",
                    "page": page_num,
                    "text": text,
                }

                if model:
                    yield model(**d)
                else:
                    yield d

        # Extract images from a PDF using PyMuPDF (fitz)
        def extract_images(pdf_path, describe_images=True):
            min_size = self._min_image_parsing_size
            with fitz.open(stream=pdf_path) as pdf_document:
                res.utils.logger.debug(
                    f"Processing pdf images from {pdf_document.page_count} pages"
                )
                for page_num in range(pdf_document.page_count):
                    page = pdf_document.load_page(page_num)

                    image_list = page.get_images(full=True)
                    for img in image_list:
                        xref = img[0]
                        base_image = pdf_document.extract_image(xref)
                        image_bytes = base_image["image"]
                        image = Image.open(io.BytesIO(image_bytes))

                        if image.size[0] < min_size[0] or image.size[1] < min_size[1]:
                            res.utils.logger.info(
                                f"Skipping image size less than {min_size}"
                            )
                            continue
                        did = res.utils.res_hash(uri.encode())
                        image_uri = f"s3://res-data-platform/samples/images/rag/docs/{did}/{page_num}/{res.utils.res_hash()}.png"
                        s3.write(image_uri, image)
                        # generate an image uri on s3 and save the image
                        # filter small images like logos

                        res.utils.logger.info(f"Processing image of size {image.size}")

                        d = {
                            "uri": uri,
                            "doc_id": name,
                            "name": f"{name}_page_{page_num}",
                            "page": page_num,
                            "text_node_type": "PDF_IMAGE_CONTENT",
                            "image_content_uri": image_uri,
                            "text": self.get_image_description(image_uri)
                            if describe_images
                            else "no description given",
                        }

                        if model:
                            yield model(**d)
                        else:
                            yield d

        res.utils.logger.info(f"generating for text and images for {pdf_path}")

        for content in extract_text(pdf_path):
            yield content

        for content in extract_images(pdf_path):
            yield content

        if get_pages_as_images:
            res.utils.logger.info(
                f"generating full pdf page images and their descriptions to augment the text and image sub parsing"
            )
            # todo make sure this works in general
            for i, image in enumerate(self.get_pdf_pages_as_images(uri)):
                # save it to s3
                page_num = i + 1
                res.utils.logger.info(f"Adding page {page_num}")
                image_name = f"{name}_page_scan_{page_num}"
                resp = self.describe_image(
                    image,
                    prompt="This is a full pdf page - please extract the context with a special understanding of the layout. Please make minor comments on the layout structure",
                    name=image_name,
                )

                image_uri = resp["uri"]
                yield {
                    "uri": uri,
                    "doc_id": name,
                    "name": image_name,
                    "page": page_num,
                    "text_node_type": "PDF_PAGE_IMAGE_CONTENT",
                    "image_content_uri": image_uri,
                    "text": resp["description"],
                }
