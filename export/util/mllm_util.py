# non-stream.py
# Created by Ningxin NX2 Sun on 2024/5/19.

import os
import requests
from datetime import datetime
import hmac
import hashlib
import base64
from requests.auth import HTTPBasicAuth

req_url = "http://ec2-3-86-228-102.compute-1.amazonaws.com:1443/ai-assistant/api/v1/assistant/model/chat/MLLM"
log_judge_prompt_txt = open('log_judge_prompt_txt.txt', 'w')

def get_timestamp():
    return str(int(datetime.now().timestamp() * 1000))

def generate_sign(timestamp, app_key, app_secret):
    input_text = f"{app_key}:{timestamp}:"
    input_bytes = input_text.encode('utf-8')
    secret_bytes = app_secret.encode('utf-8')
    hmac_result = hmac.new(secret_bytes, input_bytes, hashlib.sha256).digest()
    base64_result = base64.b64encode(hmac_result)
    signature = base64_result.decode('utf-8')
    return signature

def http_post_request(vmId, prompt, img_url, llmName, stream):
    #logger.info("New HTTP Request:")
    # print("new http request:")
    timestamp = int(datetime.now().timestamp() * 1000)
    sign = generate_sign(timestamp, "vmInner", "xkyI54pLeGbTXNIM")
    #print(f"13位时间戳:{timestamp}, sign:{sign}")

    headers = {
        "sign": sign,
        "app-key": "vmInner",
        "timestamp": str(timestamp),
        "platform": "VM",
        "X-VM-Id": "cheng-test-1",
        "X-Session-Id": "6b3f0613f0a542818961f82f593f3f7c"
    }

    # Basic auth credentials
    username = "cloudpc-api-gateway"
    password = "gaBKgJSGG6475hfkb3GV"

    # Prepare the multipart form data
    request_file = {'files':open(img_url, 'rb')}

    data = {
        'prompt': prompt,
        'llmName': llmName,
        'stream': str(stream).lower(),
        'vmId': vmId
    }

    timeout_seconds = 60
    max_retries = 2

    for retry in range(max_retries):
        try:
            response = requests.post(req_url, data=data, headers=headers, auth=HTTPBasicAuth(username, password), 
                                     timeout=timeout_seconds, files=request_file)
            if response is not None:
                # print("同步请求的返回:" + str(response.text))
                # print(f"同步请求返回的状态码: {response.status_code}")
                return response
            else:
                print("Received None response. Please check your network.")
                return None
        except requests.exceptions.Timeout:
            print(f"Error: Request timed out after {timeout_seconds} seconds")
            print(f"Error: Request timed out on attempt {retry + 1} of {max_retries}")
        except Exception as e:
            print(f"Error occurred during the request: {e}")
            print(f"Retrying... ({retry + 1}/{max_retries})")

    print("All retries failed.")
    return None

def http_post_request_llm(vmId, prompt, llmName, stream):
    #logger.info("New HTTP Request:")
    # print("new http request:")
    timestamp = int(datetime.now().timestamp() * 1000)
    sign = generate_sign(timestamp, "vmInner", "xkyI54pLeGbTXNIM")
    #print(f"13位时间戳:{timestamp}, sign:{sign}")

    headers = {
        "sign": sign,
        "app-key": "vmInner",
        "timestamp": str(timestamp),
        "platform": "VM",
        "X-VM-Id": "cheng-test-1",
        "X-Session-Id": "6b3f0613f0a542818961f82f593f3f7c"
    }

    # Basic auth credentials
    username = "cloudpc-api-gateway"
    password = "gaBKgJSGG6475hfkb3GV"

    data = {
        'prompt': prompt,
        'llmName': llmName,
        'stream': str(stream).lower(),
        'vmId': vmId
    }

    timeout_seconds = 60
    max_retries = 2

    for retry in range(max_retries):
        try:
            response = requests.post(req_url, data=data, headers=headers, auth=HTTPBasicAuth(username, password), 
                                     timeout=timeout_seconds)
            if response is not None:
                # print("同步请求的返回:" + str(response.text))
                # print(f"同步请求返回的状态码: {response.status_code}")
                return response
            else:
                print("Received None response. Please check your network.")
                return None
        except requests.exceptions.Timeout:
            print(f"Error: Request timed out after {timeout_seconds} seconds")
            print(f"Error: Request timed out on attempt {retry + 1} of {max_retries}")
        except Exception as e:
            print(f"Error occurred during the request: {e}")
            print(f"Retrying... ({retry + 1}/{max_retries})")

    print("All retries failed.")
    return None

def http_post_request_llm_log(vmId, prompt, llmName, stream, req_num):
    #logger.info("New HTTP Request:")
    # print("new http request:")
    timestamp = int(datetime.now().timestamp() * 1000)
    sign = generate_sign(timestamp, "vmInner", "xkyI54pLeGbTXNIM")
    #print(f"13位时间戳:{timestamp}, sign:{sign}")

    headers = {
        "sign": sign,
        "app-key": "vmInner",
        "timestamp": str(timestamp),
        "platform": "VM",
        "X-VM-Id": "cheng-test-1",
        "X-Session-Id": "6b3f0613f0a542818961f82f593f3f7c"
    }

    # Basic auth credentials
    username = "cloudpc-api-gateway"
    password = "gaBKgJSGG6475hfkb3GV"

    data = {
        'prompt': prompt,
        'llmName': llmName,
        'stream': str(stream).lower(),
        'vmId': vmId
    }

    timeout_seconds = 60
    max_retries = 2

    for retry in range(max_retries):
        try:
            response = requests.post(req_url, data=data, headers=headers, auth=HTTPBasicAuth(username, password), 
                                     timeout=timeout_seconds)
            if response is not None:
                # print("同步请求的返回:" + str(response.text))
                # print(f"同步请求返回的状态码: {response.status_code}")
                return response
            else:
                log_judge_prompt_txt.write(f"{req_num}:{retry} error: Received None response. Please check your network.\n")
                # print("Received None response. Please check your network.")
                return None
        except requests.exceptions.Timeout:
            # print(f"Error: Request timed out after {timeout_seconds} seconds")
            # print(f"Error: Request timed out on attempt {retry + 1} of {max_retries}")
            log_judge_prompt_txt.write(f"{req_num}:{retry} error: requests.exceptions.Timeout.\n")
        except Exception as e:
            # print(f"Error occurred during the request: {e}")
            # print(f"Retrying... ({retry + 1}/{max_retries})")
            log_judge_prompt_txt.write(f"{req_num}:{retry} error: {ex}\n")
    # print("All retries failed.")
    return None

def get_action_items(text, img_url):
     # 设置请求参数
     llmName = "intern-vl"
     vmId = "1"
     stream = False

    #  prompt = (
    #      f"You are a professional assistant. \
    #       I will give you one text prompt and one picture. \
    #       The picture is generate by Stable Diffusion Models based on the sentence.\
    #       Your job is to determine whether the image contains one main object based on the sentence I give you.  \
    #       If there is only one main object, tell me one phrase what is the one main object, and a sub-sentence to describe the one main object's detail. \
    #       The respond should follow the format:\
    #       main object: ...\
    #       detail: ...\
    #       If there is no any main object, return only one word: 'scene'.\
    #       My sentence is: {text}. My picture as it. Thank you."
    #  )

    #  prompt = (
    #      f"You are a professional assistant. \
    #       I will give you one text sentence and one picture. \
    #       The picture is generate by Stable Diffusion Models based on the sentence.\
    #       Your job is to judge whether the picture can be simply divided into foreground and background, based on the picture and sentence I gave you.  \
    #       If it can be divided into foreground and background, use one phrase to describe the foreground and one phrase to describe the background. \
    #       The respond should follow the format:\
    #       foreground: ...\
    #       background: ...\
    #       If it cannot be divided into foreground and background, reply me with one word: 'scene'.\
    #       My sentence is: {text}. My picture as it. Thank you."
    #  )

     prompt = (
         f"You are a professional assistant. \
          I will give you one text sentence and one picture. \
          The picture is generate by Stable Diffusion Models based on the sentence.\
          Your job is to judge whether the picture can be simply divided into foreground and background, based on the picture and sentence I gave you.  \
          If it can be divided into foreground and background, tell me one phrase which be the most representative one main object in the foreground, tell me one phrase to describe the main object. \
          The respond should follow the format:\
          foreground: ...\
          detail: ...\
          If it cannot be divided into foreground and background, reply me with one word: 'scene'.\
          My sentence is: {text}. My picture as it. Thank you."
     )

     response = http_post_request( vmId, prompt, img_url, llmName, stream)

     if response:
         decoded_line = response.json()
        #  print(decoded_line)

         if 'result' in decoded_line and 'result' in decoded_line['result']:
             answer = decoded_line['result']['result']
             return answer
         else:
             answer = "Error: Unable to find the result in the response."
        #  print(f"生成的todolist:\n{answer}")
         return answer
     else:
        #  print("Request failed")
         return "Error: Request failed"
        #  return None


def run_proompt_forground_long(text, img_url):
    # 设置请求参数
     llmName = "intern-vl"
     vmId = "1"
     stream = False

     prompt = (
         f"You are a professional assistant. \
          I will give you one text sentence and one picture. \
          The picture is generate by Stable Diffusion Models based on the sentence.\
          Your job is to judge whether the picture can be simply divided into foreground and background, based on the picture and sentence I gave you.  \
          If it can be divided into foreground and background, tell me one phrase which be the most representative one main object in the foreground, tell me one phrase to describe the main object. \
          The respond should follow the format:\
          foreground: ...\
          detail: ...\
          If it cannot be divided into foreground and background, reply me with one word: 'scene'.\
          My sentence is: {text}. My picture as it. Thank you."
     )

     response = http_post_request( vmId, prompt, img_url, llmName, stream)

     ret_dict = dict({})

     if response:
         decoded_line = response.json()

         if 'result' in decoded_line and 'result' in decoded_line['result']:
             answer = decoded_line['result']['result'].replace('\n', '').strip()
            #  print('answer:\n', answer)
             
             if answer == 'scene':
                 ret_dict['scene'] = 'scene'
                 return ret_dict
             
             fg = answer.split('detail:')[0].replace('\n','')[len('foreground:'):].strip()
             detail = answer.split('detail:')[-1].replace('\n','').strip()   
             ret_dict['main_object'] = fg
             ret_dict['detail'] = detail
             return ret_dict
         else:
             return ret_dict                          
                 
     else:
        return ret_dict           

def run_proompt_forground_short(text, img_url):
    # 设置请求参数
     llmName = "intern-vl"
     vmId = "1"
     stream = False

     prompt = (
         f"You are a professional assistant. \
          I will give you one text sentence and one picture. \
          The picture is generate by Stable Diffusion Models based on the sentence.\
          Your job is to judge whether the picture can be simply divided into foreground and background, based on the picture and sentence I gave you.  \
          If it can be divided into foreground and background, tell me the most representative one main object in the foreground, tell me one phrase to describe the main object. \
          The respond should follow the format:\
          foreground: ...\
          detail: ...\
          If it cannot be divided into foreground and background, reply me with one word: 'scene'.\
          My sentence is: {text}. My picture as it. Thank you."
     )

     response = http_post_request( vmId, prompt, img_url, llmName, stream)

     ret_dict = dict({})

     if response:
         decoded_line = response.json()

         if 'result' in decoded_line and 'result' in decoded_line['result']:
             answer = decoded_line['result']['result'].replace('\n', '').strip()
            #  print('answer:\n', answer)
             
             if answer == 'scene':
                 ret_dict['scene'] = 'scene'
                 return ret_dict
             
             fg = answer.split('detail:')[0].replace('\n','')[len('foreground:'):].strip()
             detail = answer.split('detail:')[-1].replace('\n','').strip()   
             ret_dict['main_object'] = fg
             ret_dict['detail'] = detail
             return ret_dict
         else:
             return ret_dict                          
                 
     else:
        return ret_dict           

def run_prompt_judge_nsfw(text):
    # 设置请求参数
     llmName = "intern-vl"
     vmId = "1"
     stream = False

     prompt = (
         f"You are a professional assistant. \
          I will give you one prompt sentence. \
          Your job is to judge whether the prompt is Not Safe for Work, such as Politicians, paedophilia, bloody, violent, pornographic, unhealthy, naked, sexist, genital organs, breasts, testicles, genitals, etc. \
          If you think the highly probable is Not Safe for Work, reply me 'NSFW'. \
          If you think it is safe with a high probability, reply to me 'SFW'. \
          If you think it is uncertain, reply me 'MAY'.\
          My sentence is: {text}. Thank you."
     )

     response = http_post_request_llm( vmId, prompt, llmName, stream)

     ret_dict = dict({})

     if response:
         decoded_line = response.json()

         if 'result' in decoded_line and 'result' in decoded_line['result']:
             answer = decoded_line['result']['result'].replace('\n', '').strip()
             return answer
     else:
        return ret_dict           

def run_prompt_judge_nsfw_list(text:list, req_num:0):
    # text = ['1', '2', '3', '4']
    texts = '\n'
    for row, p in enumerate(text):
        texts = texts + f"{row} {p}\n"
    
    # texts = '\n' + '\n'.join(text) + '\n'
    # 设置请求参数
    llmName = "intern-vl"
    vmId = "1"
    stream = False

    # prompt = (
    #  f"You are a professional assistant. I will give you a list of prompt sentence.\
    # Your job is to judge whether each prompt is Not Safe for Work separately, such as Politicians, paedophilia, bloody, violent, pornographic, unhealthy, naked, sexist, genital organs, breasts, testicles, genitals, etc.\
    # If you think one prompt the highly probable is Not Safe for Work, mark as 'NSFW'.\
    # If you think one prompt the highly probable is Safe, mark as 'SFW'.\
    # If you think one prompt it is uncertain, mark as 'MAY'.\
    # My sentence as follows:\
    #  {texts}\
    # Reply me with a list corresponding to my prompt sentence list, for example: ['NSFW','SFW','MAY', ...]. Thank you.")
    # Replay me a list, each element mean one prompt result
    # exit(0)

    prompt = (
     f"You are a professional assistant. I will give you a list of prompt sentence.\
    Your job is to judge which ones prompts are Not Safe for Work separately, such as Politicians, paedophilia, bloody, naked, sexist, genital organs, breasts, testicles, etc.\
    If you think one prompt is Not Safe for Work, mark as 'NSFW'.\
    My sentence in format: 'numbers sentence', as follows:\
     {texts}\
    Reply me with a list which only contains the numbers of all Safe for Work text prompt. \
    Thank you.")

    # print(prompt)

    response = http_post_request_llm_log( vmId, prompt, llmName, stream,req_num)

    if response:
        decoded_line = response.json()

        if 'result' in decoded_line and 'result' in decoded_line['result']:
            answer = decoded_line['result']['result'].replace('\n', '').strip()
            return answer
        else:
            return None
    else:
        return None           


if __name__ == '__main__':
    text_prompt = "An up-close portrait of a beautiful hot blonde woman sitting on a picnic bench, sun hat, sunset background, grass, nature"
    img_url = "assets/imgs/case3.png"

    # text_prompt = " a liminal space"
    # img_url = "assets/imgs/e3762b68-3239-48bb-880c-c35d90878c76.png"

    # text_prompt = "1980's photo of a family of rednecks with extreme Mullets"
    # img_url = "assets/imgs/case2.png"

    # text_prompt = "skycastle in the clouds"
    # img_url = "assets/imgs/case1.png"

    # text_prompt = "skycastle in the clouds"
    # img_url = "assets/imgs/case4.png"

    # text_prompt = "An extra dimensional object with sacred geometry design made of bismuth with alien glyphs"
    # img_url = "assets/imgs/case5.png"

    if not os.path.exists(img_url):
        print("Not found image: ", img_url)
        exit(0)
    ret = run_proompt_forground_short(text_prompt, img_url)
    print(ret)
    
    # main_object = ret.split('detail:')[0].replace('\n','')[len('main object:'):].strip()
    # detail = ret.split('detail:')[-1].replace('\n','').strip()    
    # print(main_object)
    # print(detail)

    # if ret == 'scene':
    #     print('SCENE:', ret)
    # else:
    #     ret = ret.replace('\n', '')
    #     fg = ret.split('background:')[0].replace('\n','')[len('foreground:'):].strip()
    #     bg = ret.split('background:')[-1].replace('\n','').strip()
    #     print(fg)
    #     print(bg)
    
    # if ret == 'scene':
    #     print('SCENE:', ret)
    # else:
    #     ret = ret.replace('\n', '')
    #     fg = ret.split('detail:')[0].replace('\n','')[len('foreground:'):].strip()
    #     detail = ret.split('detail:')[-1].replace('\n','').strip()     
    #     print(fg)
    #     print(detail)
