from transformers import pipeline
import os
import torch
import json
from tqdm import tqdm

work_index = 9
log_txt_name = f'nsfw_prompt_work_log_{work_index}.txt'
log_file = open(log_txt_name, 'w')

device = 'cuda' if torch.cuda.is_available() else 'cpu'
classifier = pipeline("sentiment-analysis", model="/dfs/data/pths/nsfw_prompt_classifier", device=device)

jsons_root_path = '/dfs/data/sketch_data/clear_version/jsons/'
save_jsons_root_path = '/dfs/data/sketch_data/clear_version/py_nsfw/nsfw_prompt/'

work_json_name_list=[]
for j_name in os.listdir(jsons_root_path):
    if j_name.endswith('.json'):
        if (int(j_name.split('-')[1]) % 10) == work_index:
            work_json_name_list.append(j_name)

# for debug
# work_json_name_list = work_json_name_list[0:1]

for j_name in tqdm(work_json_name_list):
    log_file.write(j_name + " begin.\n")
    json_file = open(jsons_root_path + j_name, 'r')

    save_result_json_file = open(save_jsons_root_path + "prompt_nsfw_" + j_name, 'w')

    lines = json_file.readlines()
    prompt_list = []
    for line in lines:
        line_dict = json.loads(line)
        prompt_list.append(line_dict['caption'][:510])
    log_file.write(f"prompt length:{len(prompt_list)}\n")
    result = classifier(prompt_list)
    log_file.write(f"result length:{len(result)}\n")
    assert len(prompt_list) == len(result), f"prompt_list,{len(prompt_list)} != result, {len(result)}"    
    
    for i in range(len(prompt_list)):
        row_info = {}
        row_info['file'] = j_name
        row_info['prompt'] = prompt_list[i]
        row_info['prompt_md5'] = json.loads(lines[i])['caption_md5']        
        # 如果超过512，注意提示不可信。
        row_info['score_DistilRoBERTa_Classification'] = dict(result[i])['score']        
        row_str = json.dumps(row_info)
        save_result_json_file.write(row_str)
        save_result_json_file.write('\n')
        save_result_json_file.flush()
    log_file.write(j_name + " finish.\n")
    

print('Done!')    
