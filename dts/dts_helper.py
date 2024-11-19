import requests
import json
import random
import time

auth_token = 'asgdae4wgawhgaertgwhdnhjexssarx'
# 任何请求的 最大 尝试 次数
max_try = 10

login_url = 'http://127.0.0.1:19972/dts/hi'
is_done_url = 'http://127.0.0.1:19972/dts/done'
gimgs_url = 'http://127.0.0.1:19972/dts/gimgs'

'''
cuda_name in ['cuda:0', 'cuda:1','cuda:2','cuda:3']
'''
def login(work_space:str, cuda_name:str):
    try_num = 0
    cid = "".join(random.choices(auth_token, k=15))
    login_para = {'t':auth_token, 'cid':cid, 'ws':work_space, 'cudan':cuda_name}
    print(f"[ info ] Login: {login_para}")
    
    while True:
        try:
            try_num = try_num + 1
            res = requests.get(login_url, params=login_para)
            if res.ok:
                res_text_dict = json.loads(res.text)                
                login_para['access_token'] = res_text_dict['at']
                login_para['sid'] = res_text_dict['sid']                
                return login_para
            time.sleep(1)
            if try_num > max_try:
                print(f"[ error ] Login Error: {try_num} >= {max_try}")                
                return None
        except Exception as ex:
            print(f"[ error ] Login Error: {ex}")
            return None

def is_done(login_result:dict) -> bool:
    try_num = 0
    is_done_para = {'t':login_result['access_token'], 'cid':login_result['cid'], 'sid':login_result['sid']}
    print(f"[ info ] Is_done: {is_done_para}")

    while True:
        try:
            try_num = try_num + 1
            res = requests.get(is_done_url, params=is_done_para)            
            if res.ok:
                return json.loads(res.text)['last'] <= 0
            time.sleep(1)
            if try_num > max_try:
                print(f"[ error ] Is_done Error: {try_num} >= {max_try}")                
                return True
        except Exception as ex:
            print(f"[ error ] Is_done Error: {ex}")
            return True

def get_imgs_path_list(login_result:dict, request_size:int = 10) -> list:    
    if is_done():
        return []
    try_num = 0
    get_imgs_para = {'t':login_result['access_token'], 'cid':login_result['cid'], 'sid':login_result['sid'], 'request_size':request_size}
    print(f"[ info ] Get_imgs_path_list: {get_imgs_para}")
    while True:
        try:
            try_num = try_num + 1
            res = requests.get(gimgs_url, params=get_imgs_para)            
            if res.ok:
                work_list = list(json.loads(res.text)['items'])
                if len(work_list) == 0:
                    print('[ info ] Get_imgs_path_list: get empty work list!')
                    return []
                return work_list
            time.sleep(1)
            if try_num > max_try:
                print(f"[ error ] Is_done Error: {try_num} >= {max_try}")                
                return []
        except Exception as ex:
            print(f"[ error ] Is_done Error: {ex}")
            return []
