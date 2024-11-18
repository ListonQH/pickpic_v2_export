import os
from PIL import Image
from transformers import AutoProcessor, AutoModel
import torch
import pickle
from tqdm import tqdm
import json

# load model

device = "cuda:1"

processor_name_or_path = "/dfs/data/pths/laion"

model_pretrained_name_or_path = "/dfs/data/pths/pickscore"
 
processor = AutoProcessor.from_pretrained(processor_name_or_path)

model = AutoModel.from_pretrained(model_pretrained_name_or_path).eval().to(device)
 
def calc_probs(prompt, images):

    # preprocess

    image_inputs = processor(

        images=images,

        padding=True,

        truncation=True,

        max_length=77,

        return_tensors="pt",

    ).to(device)

    text_inputs = processor(

        text=prompt,

        padding=True,

        truncation=True,

        max_length=77,

        return_tensors="pt",

    ).to(device)
 
 
    with torch.no_grad():

        # embed

        image_embs = model.get_image_features(**image_inputs)

        image_embs = image_embs / torch.norm(image_embs, dim=-1, keepdim=True)

        text_embs = model.get_text_features(**text_inputs)

        text_embs = text_embs / torch.norm(text_embs, dim=-1, keepdim=True)

        # score

        scores = model.logit_scale.exp() * (text_embs @ image_embs.T)[0]

        # get probabilities if you have multiple images to choose from

        # probs = torch.softmax(scores, dim=-1)

    return scores.item()



# 先读取所有的 re-caption JSON 文件

# internvl_8b_json = open('/dfs/data/intern-vl-26b/re-caption_InternVL2-8B.json','r')
# internvl_26b_json = open('/dfs/data/intern-vl-26b/re-caption_InternVL2-26B.json','r')
# internvl_40b_json = open('/dfs/data/intern-vl-26b/re-caption_InternVL2-40B.json','r')
# internvl_76b_json = open('/dfs/data/intern-vl-26b/re-caption_InternVL2-76B.json','r')

# cogvlm_19b_json = open('/dfs/data/CogVLM/CogVLM-llamma3-19B.json','r')
# cogvlm_19b_json = open('/dfs/data/Qwen/Qwen2-VL-7B-Instruct.json','r')
# cogvlm_19b_json = open('/dfs/data/Qwen/Qwen2-VL-7B-Instruct_w_history.json','r')
# cogvlm_19b_json = open('/dfs/data/internvl/re-caption_InternVL2-8B_w_history.json','r')
cogvlm_19b_json = open('/dfs/data/internvl/re-caption_InternVL2-8B-random_image_caption_pair-1600-1024-V0.json','r')
# cogvlm_19b_json = open('/dfs/data/internvl/re-caption_InternVL2-8B_multi_image.json','r')


a, b, c, d, e, f = 0, 0, 0, 0, 0, 0
average = 0
all_lines = cogvlm_19b_json.readlines()

for line in tqdm(all_lines):
    temp_info = json.loads(line)
    img_path = temp_info['img']
    old_caption = temp_info['old_caption']
    only_image_recaption = temp_info['only_image_recaption']
    combine_old_caption_image = temp_info['combine_old_caption_image']
    a = a + calc_probs(only_image_recaption, [Image.open(img_path)])   
    b = b + calc_probs(combine_old_caption_image, [Image.open(img_path)])   
    # c = c + calc_probs(old_caption + only_image_recaption, [Image.open(img_path)])   
    # d = d + calc_probs(old_caption + combine_old_caption_image, [Image.open(img_path)])   
    # e = e + calc_probs(only_image_recaption + old_caption, [Image.open(img_path)])   
    # f = f + calc_probs(combine_old_caption_image + old_caption, [Image.open(img_path)])  
    average = average +  calc_probs(old_caption, [Image.open(img_path)])  


print(f"a : {a/len(all_lines)}\nb : {b/len(all_lines)}\nc : {c/len(all_lines)}\nd : {d/len(all_lines)}\ne : {e/len(all_lines)}\nf : {f/len(all_lines)}\nave : {average/len(all_lines)}")

print('Done!')
