import pyarrow.parquet as pq
import cv2
import numpy as np
import os
import json
from tqdm import tqdm
import util_md5
import pickle

file_index_num = 0

# parquet_file_root_path = '/dfs/data/sketch_data/train/'
parquet_file_root_path = 'G:/code/pickpic_v2_export/parquet/'
img_save_root_path = './outputs/'

# log_files_save_root = '/dfs/data/sketch_data/clear_version/logs/'
# json_files_save_root = '/dfs/data/sketch_data/clear_version/jsons/'

log_files_save_root = './logs/'
json_files_save_root = './jsons/'

work_list = []
print('*'*50)
print(f'\t Group: {file_index_num} begin ... \t')
print('*'*50)
for i in os.listdir(parquet_file_root_path):
    if i.endswith(".parquet"):
        file_index = int(i.split('-')[1])
        if file_index == file_index_num:
            work_list.append(i)

for f in work_list:
    log_file=open(f'{log_files_save_root}{f}.txt', 'w')
    json_file = open(f'{json_files_save_root}{f}.json', 'w')

    log_file.write(f'Begin: {f} \n')

    parquet_file = pq.ParquetFile(parquet_file_root_path + f)
    table = parquet_file.read()
    best_image_uid_col = table['best_image_uid']
    caption_col = table['caption']
    image_0_uid_col = table['image_0_uid']
    image_1_uid_col = table['image_1_uid'] 
    jpg_0_col = table['jpg_0']
    jpg_1_col = table['jpg_1']

    rows = len(table.column('best_image_uid'))
    log_file.write(f'Rows: {rows} \n')

    for row in tqdm(range(rows)):       
        temp_dict = {}
        temp_dict['file'] = f
        temp_dict['best_image_uid'] = str(best_image_uid_col[row])
        temp_dict['caption'] = str(caption_col[row])
        caption_md5 = util_md5.get_string_md5(str=str(caption_col[row]))
        temp_dict['caption_md5'] = caption_md5

        temp_dict['image_0_uid'] = str(image_0_uid_col[row])
        temp_dict['image_1_uid'] = str(image_1_uid_col[row])

        img_0_bytes = jpg_0_col[row].as_buffer()
        image_array = np.frombuffer(img_0_bytes, np.uint8)
        image_0_data = cv2.imdecode(image_array, cv2.IMREAD_COLOR)
        image_0_md5 = util_md5.get_np_array_md5(image_0_data)
        temp_dict['0_md5'] = image_0_md5
        temp_dict['0_save_name'] = caption_md5 + '+' + image_0_md5 +'.png'

        if os.path.exists(img_save_root_path + temp_dict['0_save_name']):
            log_file.write(f"row:{row}, file: {temp_dict['0_save_name']} exists!\n")
        else:
            cv2.imwrite(img_save_root_path + temp_dict['0_save_name'], image_0_data)

        img_1_bytes = jpg_1_col[row].as_buffer()
        image_array = np.frombuffer(img_1_bytes, np.uint8)
        image_1_data = cv2.imdecode(image_array, cv2.IMREAD_COLOR)
        image_1_md5 = util_md5.get_np_array_md5(image_1_data)
        temp_dict['1_md5'] = image_1_md5
        temp_dict['1_save_name'] = caption_md5 + '+' + image_1_md5 +'.png'
        
        if os.path.exists(img_save_root_path + temp_dict['1_save_name']):
            log_file.write(f"row:{row}, file: {temp_dict['1_save_name']} exists!\n")
        else:
            cv2.imwrite(img_save_root_path + temp_dict['1_save_name'], image_1_data)

        log_file.write(f'row: {row} done.\n')
        
        json.dump(temp_dict, json_file)
        # json_str = json.dumps(temp_dict)
        # json_file.write(json_str)
        json_file.write('\n')
    log_file.write(f'Finish: {f} \n\n')

    log_file.flush()
    json_file.flush()
    log_file.close()
    json_file.close()
    
print('*'*50)
print(f'\t Group: {file_index_num} done! ... \t')
print('*'*50)
