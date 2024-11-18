import os
import json

'''
{
"61225763276ccb9c843d33affeb4f5c3+82127895efe44b60720c553144e5d4ff.png": 
{"img_name": "61225763276ccb9c843d33affeb4f5c3+82127895efe44b60720c553144e5d4ff.png", 
"source_file": "test-00000-of-00014-387db523fa7e7121.parquet", 
"save_path": "/dfs/data/sketch_data/clear_version/imgs/test-00000-of-00014-387db523fa7e7121/", 
"vit_detector": "sfw", "filterization_model": "safe", "vit_classification": "normal", 
"pic_score": 19.90730094909668, "clip_score": 0.7353515625, "art_score": 6.044918537139893}
}
'''
# all_imgs_infos_json = r'D:\codes\pickpic_v2_export\dts\assets\all_imgs_info_2024-11-11-09-55-01.json'
all_imgs_infos_json = r'C:\code\pickpic_v2_export\dts\assets\all_imgs_info_2024-11-11-09-55-01.json'

def select_imgs_by_score() -> list:
    ret = []
    try:
        with open(all_imgs_infos_json, 'r') as f:
            data = json.load(f)
            for k, v in data.items():
                ret.append(v['save_path'] + k)     
    except Exception as ex:
        print(f'[ Error ]: {ex}') 
        ret = []
    return ret

def select_all_imgs():
    ret = {}
    pass

# nsfw_score in [1, 2, 3].
# nsfw_score mean nsfw number.
def select_imgs_by_nsfw_score(nsfw_score:int):
    ret = {}
    pass

def select_all_prompts():
    pass

def select_all_sfw_prompt():
    pass

def select_all_sfw_imgs():
    pass


