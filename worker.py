import pyarrow.parquet as pq
import cv2
import os
from tqdm import tqdm
import numpy as np
from multiprocessing import Pool
from util_sqlite import SqliteUtil
from util_md5 import get_string_md5, get_np_array_md5
import time


def process_work(pq_file_path:str):
    
    db_helper = SqliteUtil()
    
    # open the file
    parquet_file = pq.ParquetFile(pq_file_path)
    table = parquet_file.read()
    # data
    best_image_uid_col = table['best_image_uid']
    caption_col = table['caption']
    image_0_uid_col = table['image_0_uid']
    image_1_uid_col = table['image_1_uid'] 
    jpg_0_col = table['jpg_0']
    jpg_1_col = table['jpg_1']
    
    current_file_rows = len(table.column('best_image_uid'))
    
    print(f'[ Info ] Begin export: {pq_file_path}. Rows: {current_file_rows}')
    
    for row in tqdm(range(current_file_rows)):
        prompt = str(caption_col[row].as_py())
        # prompt = prompt.replace('"', '/"')
        prompt_md5 = get_string_md5(prompt)
        
        best_uid = best_image_uid_col[row].as_py()
        img_0_bytes = jpg_0_col[row].as_buffer()
        img_0_uid = image_0_uid_col[row].as_py()
        img_0_md5 = get_np_array_md5(img_0_bytes)
        
        img_1_bytes = jpg_1_col[row].as_buffer()
        img_1_md5 = get_np_array_md5(img_1_bytes)
        img_1_uid = image_1_uid_col[row].as_py()
        
        img_0_bytes = jpg_0_col[row].as_buffer()
        image_array = np.frombuffer(img_0_bytes, np.uint8) 
        image = cv2.imdecode(image_array, cv2.IMREAD_COLOR) 
        cv2.imwrite(f'./exported_img/{img_0_md5}+{prompt_md5}.png', image)
        
        infos = dict({'source_file_name':pq_file_path.split('/')[-1], 
            'best_image_uid':best_uid, 
            'caption':prompt, 
            'caption_md5':prompt_md5, 
            'image_uid':img_0_uid, 
            'image_md5':img_0_md5, 
            'partner_img_uid':img_1_uid, 
            'partner_img_md5':img_1_md5, 
            'self_better':'true' if best_uid == img_0_uid else 'false'
            })
        
        db_helper.insert(infos)
        
        image_array = np.frombuffer(img_1_bytes, np.uint8) 
        image = cv2.imdecode(image_array, cv2.IMREAD_COLOR) 
        cv2.imwrite(f'./exported_img/{img_1_md5}+{prompt_md5}.png', image)
        
        infos['image_uid'] = img_1_uid
        infos['image_md5'] = img_1_md5
        infos['partner_img_uid'] = img_0_uid
        infos['partner_img_md5'] = img_0_md5
        infos['self_better'] = 'true' if best_uid == img_1_uid else 'false'
        db_helper.insert(infos)
    
    print(f'[ Info ] Finish export: {pq_file_path}. Rows: {current_file_rows}')

def test(str):
    print(str)
    time.sleep(10)    
    
    
def main():
    file_root_path = './parquet/'
    file_type = '.parquet'    
    img_save_path = './exported_img/'
    
    if not os.path.exists(img_save_path):
        os.mkdir(img_save_path)
        print(f'[ Warn ] Image save path: {img_save_path} not exists, create one. ')
    
    pool = Pool(3)
    
    for pq_file in os.listdir(file_root_path):
        if not pq_file.endswith(file_type):
            continue
                  
        # pool.apply_async(func=process_work, args=(file_root_path + pq_file))        
        pool.apply_async(func=process_work, args=(file_root_path + pq_file, ))        
    
    pool.close()
    pool.join()
        
if __name__ == '__main__':    
    main()
        