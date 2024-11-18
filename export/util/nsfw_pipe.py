from transformers import ViTImageProcessor, AutoModelForImageClassification, ViTImageProcessor
from PIL import Image
import torch
import os
import pickle
from tqdm import tqdm
import cv2
import json


device = 'cuda' if torch.cuda.is_available() else 'cpu'
work_index = 'test'
log_txt_name = f'nsfw_work_log_{work_index}.txt'
log_file = open(log_txt_name, 'w')
jsons_root_path = '/dfs/data/sketch_data/clear_version/jsons/'
imgs_root_path = '/dfs/data/sketch_data/clear_version/imgs/'

log_file.write(f'group {work_index} begin \n')

work_dir_name_list=[]
for j_name in os.listdir(jsons_root_path):
    if j_name.endswith('.json'):
        temp_dir_name = j_name.split('.')[0]
        if temp_dir_name.starswith(work_index):
            work_dir_name_list.append(temp_dir_name)

# make up models
# processor = ViTImageProcessor.from_pretrained(r"/dfs/data/pths/vit-base-nsfw-detector")
# https://huggingface.co/AdamCodd/vit-base-nsfw-detector
vit_based_nsfw_detector = AutoModelForImageClassification.from_pretrained(r"/dfs/data/pths/vit-base-nsfw-detector").eval().to(device)
p1 = ViTImageProcessor.from_pretrained(r"/dfs/data/pths/vit-base-nsfw-detector")

# https://huggingface.co/whizystems/NSFW-filterization/tree/main
filterization_model = AutoModelForImageClassification.from_pretrained(r"/dfs/data/pths/filterization_nsfw").eval().to(device)
p2 = ViTImageProcessor.from_pretrained(r"/dfs/data/pths/filterization_nsfw")

# https://huggingface.co/Falconsai/nsfw_image_detection
vit_based_nsfw_classification = AutoModelForImageClassification.from_pretrained(r"/dfs/data/pths/vit_base_nsfw_classification/").eval().to(device)
p3 = ViTImageProcessor.from_pretrained(r"/dfs/data/pths/vit_base_nsfw_classification/")

# from transformers import pipeline
# prompt_classifier = pipeline("sentiment-analysis", model="/dfs/data/pths/nsfw_prompt_classifier", device=device)

for i, work_dir in enumerate(work_dir_name_list):
    print(f"{i+1}/{len(work_dir_name_list)}: {work_dir}")    
    log_file.write(f'file {work_dir}.parquet.json begin \n')

    nsfw_json = open(f'/dfs/data/sketch_data/clear_version/py_nsfw/nsfw_jsons/nsfw_{work_dir}.json', 'w')

    with open(f"{jsons_root_path}{work_dir}.parquet.json", 'r') as f:
        lines = f.readlines()
        for j, line in enumerate(tqdm(lines)):           
            try:
                temp_dict = {}
                info = json.loads(line)
                img_name = info['0_save_name']
                temp_dict['img_name'] = img_name
                img_path = f"{imgs_root_path}{work_dir}/{img_name}"
                img_data = cv2.imread(img_path)
                inputs = p1(images=img_data, return_tensors="pt").to(device)
                
                outputs = vit_based_nsfw_detector(**inputs)
                logits = outputs.logits
                predicted_class_idx = logits.argmax(-1).item()
                a_res = vit_based_nsfw_detector.config.id2label[predicted_class_idx]
                temp_dict['vit_detector'] = a_res

                inputs = p2(images=img_data, return_tensors="pt").to(device)
                outputs = filterization_model(**inputs)
                logits = outputs.logits
                predicted_class_idx = logits.argmax(-1).item()
                b_res = filterization_model.config.id2label[predicted_class_idx]
                temp_dict['filterization_model'] = b_res

                inputs = p3(images=img_data, return_tensors="pt").to(device)
                outputs = vit_based_nsfw_classification(**inputs)
                logits = outputs.logits
                predicted_class_idx = logits.argmax(-1).item()
                c_res = vit_based_nsfw_classification.config.id2label[predicted_class_idx]
                temp_dict['vit_classification'] = c_res
                
                log_file.write(f"{work_dir} {j} a:{a_res} b:{b_res} c:{c_res}\n")

                nsfw_str = json.dumps(temp_dict)
                nsfw_json.write(nsfw_str)
                nsfw_json.write('\n')
                nsfw_json.flush()

                temp_dict = {}
                info = json.loads(line)
                img_name = info['1_save_name']
                temp_dict['img_name'] = img_name
                img_path = f"{imgs_root_path}{work_dir}/{img_name}"
                img_data = cv2.imread(img_path)
                inputs = p1(images=img_data, return_tensors="pt").to(device)
                
                outputs = vit_based_nsfw_detector(**inputs)
                logits = outputs.logits
                predicted_class_idx = logits.argmax(-1).item()
                a_res = vit_based_nsfw_detector.config.id2label[predicted_class_idx]
                temp_dict['vit_detector'] = a_res

                inputs = p2(images=img_data, return_tensors="pt").to(device)
                outputs = filterization_model(**inputs)
                logits = outputs.logits
                predicted_class_idx = logits.argmax(-1).item()
                b_res = filterization_model.config.id2label[predicted_class_idx]
                temp_dict['filterization_model'] = b_res

                inputs = p3(images=img_data, return_tensors="pt").to(device)
                outputs = vit_based_nsfw_classification(**inputs)
                logits = outputs.logits
                predicted_class_idx = logits.argmax(-1).item()
                c_res = vit_based_nsfw_classification.config.id2label[predicted_class_idx]
                temp_dict['vit_classification'] = c_res
                
                log_file.write(f"{work_dir} {j} a:{a_res} b:{b_res} c:{c_res}\n")

                nsfw_str = json.dumps(temp_dict)
                nsfw_json.write(nsfw_str)
                nsfw_json.write('\n')
                nsfw_json.flush()

            except Exception as ex:
                print(ex)
                log_file.write(f"ERROR {work_dir} {j} ex \n")
                log_file.flush()
            # break
        # end for lines
    log_file.write(f'file {work_dir}.parquet.json end \n')    
    # end with open json


# end for all jsons

log_file.write(f'group {work_index} done \n')
log_file.flush()
print('*'*50)
print('\t\t Group: ', work_index, ' Done.')
print('*'*50)
                