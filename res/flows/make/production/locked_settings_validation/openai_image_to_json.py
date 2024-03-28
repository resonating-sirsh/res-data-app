import base64
import inspect
import traceback
import uuid
import requests
from io import BytesIO
from res.utils.secrets import secrets
import res.utils.logging
from PIL import Image
from collections import Counter
from res.utils import logger
from res.utils import ping_slack
import json
import time


slack_id_to_notify = " <@U04HYBREM28> "
slack_channel = "damo-test"

steamer_toggle_required_keys = [
    "AI- Steam Infeed-Auto Toggle Audit",
    "AI- Steam Ring Toggle Audit",
    "AI- Steam Blow Toggle Audit",
    "AI- Steam Out-let Toggle Audit",
    "AI- Steam Fan Toggle Audit",
    "AI- Steam F-Exhaust Toggle Audit",
    "AI- Steam B-Exhaust Toggle Audit",
    "AI- Steam Temp Toggle Audit",
    "AI- Steam Lamp Toggle Audit",
    "AI- Steam Out-Let ACC",
]


steamer_numeric_required_keys = [
    "AI- Steam Speed (m/min) - Found Steamer",
    "AI- Steamer Loop Length - Audit",
    "AI- Steam Blow Speed (HZ)  - Found Steamer",
    "AI- Steamer Fan Speed - Audit",
    "AI- Steamer F-Exhaust Speed - Found Steamer",
    "AI- Steamer B-Exhaust Speed - Found Steamer",
    "AI- Steamer Out Ratio - Found Steamer",
    "AI- Steamer - Current Loop",
    "AI- Steamer - Steaming Time",
    "AI- Current Production - Found Steamer",
    "AI- Timestamp on Screen - Found Steamer",
]

pretreat_pressure_guage_keys = [
    "AI-Pretreatment Left Pressure Gauge",
    "AI-Pretreatment Right Pressure Gauge",
]


steamer_toggles_prompt = f"""
    Read the supplied group of images and fill in the desired values into the json dict I have supplied. Dont include my comments in the dict response

    Respond with the included keys in a json dict filled up with either ON or OFF or empty string '' .  DONT INCLUDE ANY OTHER TEXT OTHER THAN THE JSON DICT
 
    if the box the text is found in is GREEN, then the toggle is ON, if the button is PINK or PURPLE or RED, then the toggle is OFF
    if you cannot find a box with the matching text, or you are not sure, just leave that entry in the dictionary as an empty string, instead of saying it is ON or OFF
    
     Keys to include in the json dict:
   "AI- Steam Infeed-Auto Toggle Audit",# look for text "Infeed-Auto",
    "AI- Steam Ring Toggle Audit", #look for text "Ring" 
    "AI- Steam Blow Toggle Audit",  #look for text "Blow" 
    "AI- Steam Out-let Toggle Audit", # look for text "Out-let" 
    "AI- Steam Fan Toggle Audit", # look for text "Fan" 
    "AI- Steam F-Exhaust Toggle Audit", # look for text "F-Exhaust" 
    "AI- Steam B-Exhaust Toggle Audit",   # look for text "B-Exhaust" or text "R-Exhaust"
    "AI- Steam Temp Toggle Audit",  # look for text "Temp-" 
    "AI- Steam Lamp Toggle Audit",  # look for text "Lamp"
    "AI- Steam Out-Let ACC", # look for text "Out-let ACC" 
   
    """

pretreatment_pressure_readings_prompt = f"""
    Read the image and tell me that the 2 readings are from the digital displays.
    If you are not sure or you cannot find the yellow/green digital displays, or you cannot read them, just reply with empty string in the json dict values '' .  
    DONT INCLUDE ANY OTHER TEXT OTHER THAN THE JSON DICT
 
    The digital displays are surrounded by navy colored plastic, are round, and have a yellow/green background on the digital display part.  THere is a left one and a right one.
    Dont include any measurement of what the numbers are - i just want the raw numeric value in the value of the dictionary.
    The numeric values will have a decimal place, but the decimal place might not be easy to see. So for example if you see 738, I want you to show that as 73.8
    use your own model, dont try to run python code

    Here are the values I want you to fill in

    \"AI-Pretreatment Left Pressure Gauge\" :\"\",
    \"AI-Pretreatment Right Pressure Gauge\" :\"\"
    """

system_promt = "You are an image processing AI, which looks at screens and settings and does their very best to read button numbers/settings/colors from those images, and then infers meaning based on the supplied examples, if any. Respond only in Json Dictionaries unless the instructions say otherwise. You are an expert. Do your best."

steamer_numbers_prompt_old_assume_one_screen = f"""
    Read the supplied 'Image 0', and fill in the desired values into the json dict I have supplied. 

    For any numerical answers, do not include the type of measurement. Eg for 60HZ, put that in the dict as 60, i.e number only.  

    Respond with the included json dict filled up.  DONT INCLUDE ANY OTHER TEXT OTHER THAN THE JSON DICT

    Please fill this json up for Image 0

    \"AI- Steam Speed (m/min) - Found Steamer"\": \"\", # Top middle of screen, measured in M/min. There should be a number after the decimal place, include it.
    \"AI- Steamer Loop Length - Audit\": \"\", # Look for text \"Loop Setting\" on VERY LEFT HAND SIDE of the screen with a white background, measured in mm. IT IS NOT "CURRENT LOOP" with yellow background
    \"AI- Steam Blow Speed (HZ)  - Found Steamer\": \"\", #This DIRECTLY TO THE RIGHT of 'Blow Speed' text, measured in HZ, left hand side of screen
    \"AI- Steamer Fan Speed - Audit\": \"\", #This is DIRECTLY TO THE RIGHT of 'Fan Speed', measured in HZ, left hand side of screen
    \"AI- Steamer F-Exhaust Speed - Found Steamer\": \"\", #'This is DIRECTLY TO THE RIGHT of F-Exhaust Speed', measured in HZ, left hand side of screen
    \"AI- Steamer B-Exhaust Speed - Found Steamer\": \"\", #'This is DIRECTLY TO THE RIGHT of B-Exhaust Speed', measured in HZ, left hand side of screen
    \"AI- Steamer Out Ratio - Found Steamer\": \"\", #  DIRECTLY TO THE RIGHT of 'Out Ratio', middle left of screen
    \"AI- Steamer - Current Loop\": \"\", #  text in a yellow box directly below the text "Current Loop", middle of screen, measured in MM
    \"AI- Steamer - Steaming Time\": \"\", #  text in a yellow box directly below the text "Steaming Time", middle of screen, numbers only
    \"AI- Current Production - Found Steamer\": \"\", #  text in a yellow box directly below the text "Current Production", middle of screen, meausred in m
    \"AI- Timestamp on Screen - Found Steamer\": \"\", #  The datetime visible in the top right hand side of the screen

    """

relax_dry_multi_screen = f"""
    Read the supplied images, and fill in the desired values into the json dict I have supplied. 

    For any numerical answers, do not include the type of measurement. Eg for 60HZ, put that in the dict as 60, i.e number only.  The exceptoin to this is percetage i.e %. include percentages where you find them

    Respond with the included json dict filled up.  DONT INCLUDE ANY OTHER TEXT OTHER THAN THE JSON DICT

    You will need to find the image of a TV screen that has a blue background, and may have barely visible text saying "SPEED MENU" across the top.  THe black frame of the screen will say 'Delta'. On this screen find the text I have indicated per setting.

    Please fill this json up for Image.  If you are not sure of a value or cannot find it - leave the value as empty string. do not guess

    "AI- Relax Dry: Temperature - Audit", 
    "AI- Relax Dry: Pilot Speed - Audit",
    "AI- Relax Dry: Centering Speed % - Audit", # Text is 'Centering' and is on top right hand side of screen. is a % figure, though you may not see the % sign. return it as a %
    "AI- Relaxed Dryer - Rolling Mill", # Text is 'Rolling Mill' and is on top right hand side of screen. is a % figure, though you may not see the % sign. return it as a %
    "AI- Relaxed Dryer - Overfeed %", # Text is 'overfeed' and is on middle right hand side of screen. is a % figure, though you may not see the % sign. return it as a %
    "AI- Relaxed Dryer - Wire Belt 2 and 3", # Text is '2 - 3 wire belt' and is on middle right hand side of screen. is a % figure, though you may not see the % sign. return it as a %
    "AI- Relaxed Dryer - Wire Belt 4 and 5",  # Text is '4 - 5 wire belt' and is on bottom right hand side of screen. is a % figure, though you may not see the % sign. return it as a %
    "AI- Relaxed Dryer - Plaiter", # Text is 'plaiter' and is on bottom right hand side of screen. is a % figure, though you may not see the % sign. return it as a %
    "AI- Relaxed Dryer - Exhaust Blower", # Text is 'exhaust blower' and is on top left hand side of screen. is a % figure, though you may not see the % sign. return it as a %
    "AI- Relaxed Dryer - Heat Blowers 1 and 2",  # Text is '1# Heat Blower' and is on top left hand side of screen. is a % figure, though you may not see the % sign. return it as a %
    "AI- Relaxed Dryer - Heat Blowers 3 and 4",  # Text is '3# Heat Blower' and is on middle left hand side of screen. is a % figure, though you may not see the % sign. return it as a %
    "AI- Relaxed Dryer - Heat Blowers 5 and 6",  # Text is '5# Heat Blower' and is on bottom left hand side of screen. is a % figure, though you may not see the % sign. return it as a %
    """


steamer_multiple_image_prompt = f"""
    Attached are a list of images, in no particular order.  I need you to look at all the images and attempt to fill in the supplied json dict.  The data I am asking for could be in any of the images. I will describe what the image looks like, and within that image, where the data I want is.

    For any numerical answers, do not include the type of measurement. Eg for 60HZ, put that in the dict as 60, i.e number only.  
    For any answer, if you dont know, cant see, or are not sure in any way, leave the key value blank, i.e empty string

    Respond with the included json dict filled up.  DONT INCLUDE ANY OTHER TEXT OTHER THAN THE JSON DICT


    \n Please fill this json up.  The comment on how the find the value comes first, and then the key where I want you to put that value comes immediately after the comment.

     # Which image number looks like a TV control panel that has at least a 50% of these bits of text on it \"Running\", \"Stop\", \"Fan Speed\", \"Current-Production\", \"Fan\",  \"Ring\", \"Blow\",  \"Out Ratio\",  \"Blow Speed\", \"Zero Clearing\"
    \"AI- Steamer Main Screen Image Number\": \"\",

    # Which image number has just ONE black digital display on it, with text 'OMRON' on the display. This display is the only thing on a mostly grey/silver metal panel"
    \"AI- Steamer Single Digital Display Image Number\": \"\",

    # Look for the image that has either 2 or 3 digital number displays on it - you should probably see text "REGULADOR DE PRESION" on the image. Read the green digital text from the black panel ABOVE the text "Medidor Presion de Vapor en Mpa".  
    # The display should be be rougly at the top of the photo. 
    \"AI- Steamer Manifold Pressure - Audit\": \"\", 
    
    #  Look for the image that has either 2 or 3 digital number displays on it - you should probably see text "REGULADOR DE PRESION" on the image. There is a lower display that has red digits on its top half and green digits in its bottom half.  I want the red digits from the top half of this panel.  This number is always displayed with the last digit being after the decimal place. 
    # eg if you think you are seeing 99.99 or 9999, its more likely to be 999.9, note the decimal has moved in my example
    \"AI- Steamer - Red Temperature - Audit\": \"\", 

    #Look for the image that has 2 or 3 digital number displays on it - you should see text "REGULADOR DE PRESION" on the image. There is a lower display that has red digits on its top half and green digits in its bottom half.  I want the green digits from the bottom half of this panel. This number is always displayed with the last digit being after the decimal place. eg if you think you are seeing 99.99 or 9999, its more likely to be 999.9 - note the decimal has moved
    \"AI- Steamer - Green Temperature - Audit\": \"\", 
    

    """

steamer_numbers_prompt_single_display = f"""
   # 
    #This digital display has green digit(s) on its lower half and red digit(s) in its top half.  I want the  reading from the bottom half of this panel. These digits will be much smaller than those digits from the top half of panel
    \"AI- Steamer - Green Other\": \"\", 
    
    #This  digital display has green digit(s) on its lower half and red digit(s) in its top half.  I want the reading from the top half of this panel.
    \"AI- Steamer - Red Other\": \"\", 
"""


# Function to encode the image
def encode_image_from_filesystem(image_path):
    try:
        with open(image_path, "rb") as image_file:
            return base64.b64encode(image_file.read()).decode("utf-8")
    except Exception as ex:
        res.utils.logger.error(f"Could not load {image_path}")
        return None


def encode_image_from_url(image_url):
    response = requests.get(image_url)
    if response.status_code == 200:
        return base64.b64encode(BytesIO(response.content).read()).decode("utf-8")
    else:
        err_msg = f"encode_image_from_url openai - Error fetching image: HTTP {response.status_code} "
        ping_slack(err_msg, slack_channel)
        raise Exception(err_msg)


def get_current_function_name():
    return inspect.currentframe().f_back.f_code.co_name


def get_steamer_multiple_photos_gpt(uriImagesToReads: list, roll_flow_info=""):
    steamer_first_response = generic_convert_image_to_json_gpt(
        uriImagesToReads,
        False,
        steamer_multiple_image_prompt,
        validate_steamer_numbers_multi_photos,
        get_current_function_name(),
        roll_flow_info=roll_flow_info,
    )

    steamer_main_screen_numbers_json = dict()
    steamer_main_screen_toggles_json = dict()
    steamer_single_display_json = dict()
    main_screen_image_index = steamer_first_response.get(
        "AI- Steamer Main Screen Image Number"
    )
    if main_screen_image_index and main_screen_image_index != "":
        main_screen_image_index = int(main_screen_image_index)
        steamer_main_screen_numbers_json = get_steamer_main_screen_numbers_gpt(
            uriImagesToReads[main_screen_image_index]
        )
        steamer_main_screen_toggles_json = get_steamer_main_screen_toggles_gpt(
            uriImagesToReads[main_screen_image_index]
        )

    single_digital_display_image_index = steamer_first_response.get(
        "AI- Steamer Single Digital Display Image Number"
    )
    if single_digital_display_image_index and single_digital_display_image_index != "":
        single_digital_display_image_index = int(single_digital_display_image_index)
        steamer_single_display_json = get_steamer_single_digital_display_numbers_gpt(
            uriImagesToReads[single_digital_display_image_index]
        )

    steamer_first_response.update(steamer_main_screen_numbers_json)
    steamer_first_response.update(steamer_main_screen_toggles_json)
    steamer_first_response.update(steamer_single_display_json)
    res.utils.logger.info(res.utils.safe_json_dumps(steamer_first_response))
    return steamer_first_response


def get_steamer_main_screen_numbers_gpt(uriImageToReads, roll_flow_info=""):
    # example_image = "./res/flows/make/production/locked_settings_validation/steam1.jpg"
    steamer_numbers_json = generic_convert_image_to_json_gpt(
        [uriImageToReads],
        False,
        steamer_numbers_prompt_old_assume_one_screen,
        validate_steamer_numbers,
        get_current_function_name(),
        roll_flow_info=roll_flow_info
        # example_image,
    )
    return steamer_numbers_json


def get_relax_dry_control_panel_gpt(uriImageToReads, roll_flow_info=""):
    # example_image = "./res/flows/make/production/locked_settings_validation/steam1.jpg"
    relax_dry_json = generic_convert_image_to_json_gpt(
        uriImageToReads,
        False,
        relax_dry_multi_screen,
        no_validation,
        get_current_function_name(),
        roll_flow_info=roll_flow_info
        # example_image,
    )
    return relax_dry_json


def get_steamer_single_digital_display_numbers_gpt(uriImageToReads, roll_flow_info=""):
    steamer_numbers_json = generic_convert_image_to_json_gpt(
        [uriImageToReads],
        False,
        steamer_numbers_prompt_single_display,
        no_validation,
        get_current_function_name(),
        roll_flow_info=roll_flow_info,
    )
    return steamer_numbers_json


def get_steamer_main_screen_toggles_gpt(uriImageToRead, roll_flow_info=""):
    steamer_toggles_json = generic_convert_image_to_json_gpt(
        [uriImageToRead],
        True,
        steamer_toggles_prompt,
        validate_steamer_toggles,
        get_current_function_name(),
        image_to_divide_index=0,
        roll_flow_info=roll_flow_info,
    )

    return steamer_toggles_json


def get_pretreat_double_pressure_gauges_gpt(uriImageToRead, roll_flow_info=""):
    pretreat_pressure_gauges_json = generic_convert_image_to_json_gpt(
        uriImageToRead,
        False,
        pretreatment_pressure_readings_prompt,
        confirm_all_valid_and_all_keys_present_pretreat_pressure_gauge,
        get_current_function_name(),
        roll_flow_info=roll_flow_info,
    )

    return pretreat_pressure_gauges_json


def generic_convert_image_to_json_gpt(
    uriImagesToRead: list,
    divide_images_needed: bool,
    prompt,
    validator_func,
    caller_info: str,
    local_example_image_path: str = None,
    image_to_divide_index: int = -1,
    roll_flow_info="",
):
    """
    Generic handler for converting image to Json dict.  need to supply a validator function which should return None (not valid) or a json dict (validated)

    """
    try:
        api_key = secrets.get_secret("ASK_ONE_OPENAI_API_KEY")
        validated = None
        # first load local example image into array if present, then either load image sub divisions, or load single image. This array will be sent to GPT
        base64_images = []
        if local_example_image_path:
            local_example_image_path_b64 = encode_image_from_filesystem(
                local_example_image_path
            )
            base64_images.append(local_example_image_path_b64)

        # For determining color of buttons on a busy screen, its best to subdivide the image into many small images
        if divide_images_needed:
            imagepaths = divide_image(
                uriImagesToRead[image_to_divide_index],
                "./res/flows/make/production/locked_settings_validation/quadrants/",
            )

            for subimagepaths in imagepaths:
                base64_images.append(encode_image_from_filesystem(subimagepaths))
        else:
            # Encode the binary data to base64
            for imageUrl in uriImagesToRead:
                encoded_data_image_url_b64 = encode_image_from_url(imageUrl)
                base64_images.append(encoded_data_image_url_b64)

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        }

        messages_content = []

        for i, image in enumerate(base64_images):
            message = {
                "type": "text",
                "text": f"This is image {i}:",
            }
            messages_content.append(message)

            image_message = {
                "type": "image_url",
                "image_url": {
                    "url": f"data:image/jpeg;base64,{image}",
                    "detail": "high",
                },
            }
            messages_content.append(image_message)

        # Add your query text at the end
        messages_content.append(
            {
                "type": "text",
                "text": prompt,
            }
        )

        payload = {
            "model": "gpt-4-vision-preview",
            "messages": [
                {"role": "system", "content": system_promt},
                {
                    "role": "user",
                    "content": messages_content,
                },
            ],
            "max_tokens": 4096,
        }

        count = 0
        json_dict_resps = []

        while count < 3 and not validated:
            try:
                count += 1
                response = None
                res.utils.logger.info(
                    f"Sending request to GPT - attempt {count} for this request..."
                )

                start_time = time.time()

                response = requests.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers=headers,
                    json=payload,
                )

                end_time = time.time()
                elapsed_time = "{:.1f}".format(end_time - start_time)
                res.utils.logger.info(f"GPT response: {response}")
                res.utils.logger.info(res.utils.safe_json_dumps(response.json()))

                gptJson = (
                    response.json().get("choices")[0].get("message").get("content")
                )

                res.utils.logger.info(f"GPT call took {elapsed_time} seconds to run.")

                json_dict_from_image = gptJson.lstrip("```json\n").rstrip("```")

                json_dict_from_image = json.loads(json_dict_from_image)
                json_dict_resps.append(json_dict_from_image)
                validated = validator_func(json_dict_resps)
            except:
                import traceback

                error = (
                    f"generic_convert_image_to_json - {caller_info} error.  Trying again ({count}). Exception: {traceback.format_exc()} \n\n GPT Response:{response} {slack_id_to_notify} ",
                    slack_id_to_notify,
                )
                res.utils.logger.info(error)
                res.utils.ping_slack(error, slack_channel)
    except:
        import traceback

        error = f"generic_convert_image_to_json - {caller_info} error.  Exception: {traceback.format_exc()} {slack_id_to_notify} "

        res.utils.logger.warn(error)
        res.utils.ping_slack(error, slack_channel)

    return validated


def validate_steamer_numbers(list_json_dicts):
    valid = confirm_all_keys_in_dict(list_json_dicts[-1], steamer_numeric_required_keys)
    if valid:
        return list_json_dicts[-1]


def validate_steamer_numbers_multi_photos(list_json_dicts):
    return list_json_dicts[-1]


def no_validation(list_json_dicts):
    return list_json_dicts[-1]


def validate_steamer_toggles(list_json_dicts):
    """
    We recieve a list of either 1, 2 or 3 dicts, and we need to figure out if the list we got is good enough to return as a valid json dict of what the settings might be
    logic here is as follows
    either all keys are present AND on and off on first attempt. If so, return
    if 2 dicts, then all present and valid on second dict, but either each key matches dict1, or the value was blank on dict1, in which case dict2 is enough, so we say its valid
    if 3 dicts, we just count up the votes and hope for the best"""
    if len(list_json_dicts) == 1:
        valid = confirmall_valid_and_all_keys_pres_steamer_toggles(list_json_dicts[0])
        if valid:
            return list_json_dicts[0]

    if len(list_json_dicts) == 2:
        valid = confirmall_valid_and_all_keys_pres_steamer_toggles(list_json_dicts[1])
        if valid:
            valsMatch = checkValsMatch(list_json_dicts[0], list_json_dicts[1])
            if valsMatch:
                return list_json_dicts[1]

    if len(list_json_dicts) == 3:
        voted_dict = steamer_toggle_vote_best_results(list_json_dicts)
        return voted_dict

    return None


def steamer_toggle_vote_best_results(dicts):
    """for each key in the list of dicts, we conduct a vote to see if ON or OFF got the highest results"""
    result = {}
    for key in steamer_toggle_required_keys:
        # Count occurrences of each value for the current key
        values_count = Counter(d.get(key, "") for d in dicts)

        on_count = values_count["ON"]
        off_count = values_count["OFF"]

        if on_count > off_count:
            result[key] = "ON"
        elif off_count > on_count:
            result[key] = "OFF"
        else:
            result[key] = ""

    return result


def test_most_likely_value_final():
    dicts = [
        {
            "Infeed-Auto": "ON",
            "Ring": None,
            "Blow": "OFF",
            "Out-let": "OFF",
            "Fan": "ON",
            "F-Exhaust": "OFF",
            "B-Exhaust": "ON",
            "Temp-": "",
            "Lamp": "OFF",
            "Out-Let ACC": "ON",
        },
        {
            "Infeed-Auto": "OFF",
            "Ring": "OFF",
            "Blow": "ON",
            "Out-let": "ON",
            "Fan": "",
            "F-Exhaust": "OFF",
            "B-Exhaust": "ON",
            "Temp-": "ON",
            "Lamp": "",
            "Out-Let ACC": "ON",
        },
        {
            "Infeed-Auto": "",
            "Ring": "ON",
            "Blow": "ON",
            "Out-let": "OFF",
            "Fan": "",
            "F-Exhaust": "OFF",
            "B-Exhaust": "ON",
            "Temp-": "",
            "Lamp": "OFF",
            "Out-Let ACC": "OFF",
        },
    ]

    expected_result = {
        "Infeed-Auto": "",
        "Ring": "",
        "Blow": "",
        "Out-let": "",
        "Fan": "",
        "F-Exhaust": "ON",
        "B-Exhaust": "",
        "Temp-": "ON",
        "Lamp": "",
        "Out-Let ACC": "",
    }
    result = steamer_toggle_vote_best_results(dicts)


# Run the final corrected tes


def confirmall_valid_and_all_keys_pres_steamer_toggles(dict):
    allvalid = all(value in ["ON", "OFF"] for value in dict.values())
    allKeysPresent = confirm_all_keys_in_dict(dict, steamer_toggle_required_keys)
    return allvalid and allKeysPresent


def confirm_all_valid_and_all_keys_present_pretreat_pressure_gauge(list_dicts):
    mydict = list_dicts[-1]
    allNotEmpty = all(value not in [""] for value in mydict.values())
    allPresent = confirm_all_keys_in_dict(mydict, pretreat_pressure_guage_keys)
    if allNotEmpty and allPresent:
        return mydict
    else:
        return None


def confirm_all_keys_in_dict(given_dict, key_list):
    return all(key in given_dict for key in key_list)


def checkValsMatch(dict1, dict2: dict):
    for key in dict2.keys():
        if dict1.get(key) != dict2.get(key):
            if dict1.get(key) in ("ON", "OFF"):
                return False
    return True


def divide_image(image_path, output_folder):
    """GPTV resonds far better to many smaller images than 1 large image. so we divide that up and send it off"""
    import os
    import shutil

    specificfolder = str(uuid.uuid4())[-8:]
    output_folder += f"\\splitimages_{specificfolder}"

    os.makedirs(output_folder, exist_ok=True)

    list_image_paths = []
    response = requests.get(image_path)
    # Open the image
    with Image.open(BytesIO(response.content)) as img:
        # Calculate dimensions for cropping
        width, height = img.size
        img = img.resize((int(width * 0.30), int(height * 0.30)))
        width, height = img.size
        overlap_w, overlap_h = width * 0.05, height * 0.05  # 5% overlap on each side

        # Calculate the boundaries for the 8 divisions
        horizontal_divisions = [0, width / 4, width / 2, 3 * width / 4, width]
        vertical_divisions = [0, height / 4, height / 2, 3 * height / 4, height]

        # Adjust boundaries for overlap
        for i in range(1, 4):
            horizontal_divisions[i] -= overlap_w
            vertical_divisions[i] -= overlap_h

        # Create a dictionary to hold the calculated coordinates
        areas = {}

        # Calculate coordinates for each area with overlap
        for i in range(4):
            for j in range(4):
                area_name = f"divided_{i}_{j}"
                left = max(0, horizontal_divisions[i] - (overlap_w if i > 0 else 0))
                upper = max(0, vertical_divisions[j] - (overlap_h if j > 0 else 0))
                right = min(
                    width, horizontal_divisions[i + 1] + (overlap_w if i < 3 else 0)
                )
                lower = min(
                    height, vertical_divisions[j + 1] + (overlap_h if j < 3 else 0)
                )
                areas[area_name] = (left, upper, right, lower)

        # Crop and save each area
        for name, coords in areas.items():
            cropped_img = img.crop(coords)
            cropped_img.save(f"{output_folder}/{name}.png")
            list_image_paths.append(f"{output_folder}/{name}.png")

    return list_image_paths


if (__name__) == "__main__":
    get_pretreat_double_pressure_gauges_gpt(
        "https://v5.airtableusercontent.com/v3/u/24/24/1704988800000/syd9P4rycLsaBMSbtSrIHQ/_OoC4Mqcnxkf2xLuXYALR-shZwGh8fy4GR9x5R0czLEjI3zOarsRvswuLzuYFraqy2LFK7XIn8_MG0TRcB7VAeOzoeMDGoz0lK7h3OuvEj8uc3dOmpALvDQL67s52GTDncWXjimp2bwL3FDHe7vwwQ/YVeDV893GPio5z4maTM-Za4VoW4N44yDZ9WvAWbLq2A"
    )
