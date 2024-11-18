from transformers import AutoTokenizer
import json
from mllm_util import *
from tqdm import tqdm

tokenizer = AutoTokenizer.from_pretrained("/dfs/data/pths/intern/tokenizer", trust_remote_code=True)

all_prompts_json = '/dfs/data/sketch_data/clear_version/res/all_prompts2024-09-09-07-14-34.json'
all_prompts_dict = dict({})
with open(all_prompts_json, 'r') as f:
    all_prompts_dict = json.load(f)
# 58934
print(len(all_prompts_dict.items()))

based_txt = "You are a professional assistant. I will give you a list of prompt sentence.\
            Your job is to judge which ones prompts are Not Safe for Work separately, such as Politicians, paedophilia, bloody, violent, pornographic, unhealthy, naked, sexist, genital organs, breasts, testicles, genitals, etc.\
            If you think one prompt is Not Safe for Work, mark as 'NSFW'.\
            My sentence in format: 'numbers sentence', as follows:\
            Reply me with a list which contains the numbers of all Safe for Work text prompt.\
            Thank you."
based_txt_len = len(tokenizer.encode(based_txt))
# 123
print(based_txt_len)

all_prompts_with_token_len = dict({})
all_prompts_with_token_len_values = list([])
for k, v in tqdm(all_prompts_dict.items(), " Calculate Token Length: "):
    prompt = v['prompt']
    token_len = tokenizer.encode(prompt)
    v['token_len'] = len(token_len)
    all_prompts_with_token_len[k] = v
    all_prompts_with_token_len_values.append(v)

sorted_prompt = sorted(all_prompts_with_token_len_values, key= lambda x:x['token_len'])
import pickle

with open('sorted_list.pkl', 'wb') as f:
    pickle.dump(sorted_prompt, f)
print(len(sorted_prompt))
exit(0)
# with open('temp.txt', 'w') as f:
#     for v in sorted_prompt:
#         f.write(f"{v}\n")

request_log = open('request_log.txt', 'w')
judge_res_json = open('judge_res_json.json', 'w')

max_prompt_len = 1024 - based_txt_len
begin_it = 0
step = 0
while True:    
    step = step + 1
    if begin_it > len(sorted_prompt):
        break
    work_list = []
    current_quality = 0
    while True:
        if begin_it > len(sorted_prompt):
            break
        
        current_quality = current_quality + 3 + sorted_prompt[begin_it]['token_len']
        
        if current_quality > max_prompt_len:
            break

        work_list.append(sorted_prompt[begin_it])
        begin_it = begin_it + 1
    
    # 开始请求    
    judge_res = run_prompt_judge_nsfw_list([i['prompt'] for i in work_list], step)
    print(f"Current Step: {step}, judge {len(work_list)} prompts, tokens: {current_quality}, respond is None = {judge_res == None} \n")
    # 失败
    if judge_res is None:
        request_log.write(f"None#{step}#{[i['caption_md5'] for i in work_list]}\n")
        continue
    # 杂乱的返回
    if len(judge_res) > len("Safe for Work text prompts:") + 3 * len(work_list):
        request_log.write(f"Unknow#{step}#{judge_res}#{[i['caption_md5'] for i in work_list]}\n")
        continue
    # 能解析的返回
    if (':' in judge_res) and (', ' in judge_res):
        judge_res = judge_res.split(':')[-1]
    judge_res = judge_res.replace(' ', '')
    judge_res = [int(i) for i in judge_res.split(',')]
    temp_dict = {}
    for i, v in enumerate(work_list):        
        if i in judge_res:
            temp_dict[v['caption_md5']] = 'sfw'
        else:
            temp_dict[v['caption_md5']] = 'nsfw' 
    judge_res_json.write(json.dumps(temp_dict))
    judge_res_json.write('\n')
    judge_res_json.flush()

print('Done !')

def get_prompt_token_len():
    all_prompts_with_token_len = dict({})
    for k, v in tqdm(all_prompts_dict.items()):
        prompt = v['prompt']
        token_len = tokenizer.encode(prompt)
        v['token_len'] = len(token_len)
        all_prompts_with_token_len[k] = v
    
    with open('all_prompts_with_token_len.json', 'w') as f:
        json.dump(all_prompts_with_token_len, f)
        f.flush()
    
    print('Done')


# if __name__ == '__main__':
#     # get_prompt_token_len()
#     print()
