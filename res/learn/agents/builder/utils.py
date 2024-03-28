import json
import res
import openai
from PIL import Image
from tenacity import retry, wait_fixed, stop_after_attempt
import typing

DEFAULT_MODEL = "gpt-4-1106-preview"


def describe_visual_image(
    url: str | typing.List[str] = None,
    question_relating_to_url_passed: str = "describe the image you see",
    **kwargs,
):
    """
    A url (or list of urls) to an image such as a png, tiff or JPEG can be passed in and inspected
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

    sign_it = lambda url: (
        url
        if "AWSAccessKeyId" in url
        else res.connectors.load("s3").generate_presigned_url(url)
    )

    if isinstance(url, str):
        url = [url]

    url = [sign_it(u) for u in url]

    res.utils.logger.debug(f"{url=}, {question_relating_to_url_passed=}")

    """
    we pass one or more urls to the LLM - often just one but sets can be useful
    """
    response = openai.chat.completions.create(
        model="gpt-4-vision-preview",
        messages=[
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": question_relating_to_url_passed},
                    *[{"type": "image_url", "image_url": u} for u in url],
                ],
            }
        ],
        max_tokens=3000,
    )

    desc = response.choices[0].message.content

    return f"Here is the description of the image as asked: {desc}"


@retry(wait=wait_fixed(5), stop=stop_after_attempt(2), reraise=True)
def describe_and_save_image(image: Image, prompt: str, uri: str):
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


def ask(question: str, model=None, as_json=False):
    """
    this is a direct request rather than the interpreter mode
    response_format={"type": "json_object"}
    """
    plan = (
        f""" Answer the users question as asked  """
        if not as_json
        else f""" Answer the users question as asked and respond in JSON format """
    )

    response_format = {"type": "json_object"} if as_json else None

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


def function_search(question, function_data):
    # Q = "I need some function to search the queue of bodies and for any bodies that have specific statuses , get their body nested by material. I would also like to see any conversations about them being discussed in the apply color queue channel "
    P = f"""
       Below are a map of functions that we have. There are different types of functions such as Vector Search, Columnar Search, API calls.
       Vector Searches are usually over text stores like Slack or the Coda wiki and can provide general commentary. 
       The columnar stores tend to me more structured and related to work queues in the Resonance Supply chain.
       API calls are useful if answering specific queries where arguments have been supplied in the form of codes, ids or specific search parameters.
       Use this information to always return a selection of the most useful functions and the rating. It is better to return more functions.
       Use the provided response schema below to respond with a list of functions to help with the user question
       
       **Question**
       ```
       {question}
       ```
       
        Please select three functions that allows for an understanding of Resonances garment construction details
    
        **Function Data**
      ```json
      {json.dumps(function_data)}
      ```
    
      ** Response Format**
    
    class FunctionInfo(BaseModel):
        name: str
        reason_for_choosing: str
        rating_percent: float
         
    class UsefulFunctions(BaseModel):
        functions: typing.List[FunctionInfo]
    """

    r = ask(P, as_json=True)
    return json.loads(r)
