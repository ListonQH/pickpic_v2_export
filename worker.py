import pyarrow.parquet as pq
import cv2
import os
from tqdm import tqdm
import numpy as np
from multiprocessing import Pool, cpu_count, Manager, Process

from util_sqlite import SqliteUtil
from util_md5 import get_string_md5, get_np_array_md5
import time
import pickle
from util_exception import WorkException


check_point_file_path = './data_export.checkpoint'
quit_work_flag = 'quit'
file_root_path = './parquet/'
file_type = '.parquet'    
img_save_path = './exported_img/'

def spy_work(queue, spy_process_exit_enent, check_point_file):
    print('[ Inof ] Spy process start ...')
    while not spy_process_exit_enent.is_set():
        if not queue.empty():
            msg:dict = queue.get(block=False)
            
            if msg['type'] == 'Break':
                msg['finish'] = False
            elif msg['type'] == 'Finish':
                msg['finish'] = True
            
            source_file_name = msg['file_name']

            check_point_file[source_file_name]=msg
            print('spy: \n', check_point_file)
            with open(check_point_file_path, 'wb') as pkl_file:
                pickle.dump(check_point_file, pkl_file)

        time.sleep(1)

    print('[ Info ] Spy process stop ...')


def process_error_callback(ex:WorkException):
    print(ex.print_error())
    
    pq_file_checkpoint = dict({
            'finish':False,
            'break_row':ex.break_row })
    global check_point_file
    check_point_file[ex.file_name] = pq_file_checkpoint

    with open(check_point_file_path, 'wb') as pkl_file:
        pickle.dump(check_point_file, pkl_file)
    
    print(f'[ Error ] Stop {ex.file_name} export, at row: {ex.break_row}.')

def finish_callback(file_name:str):

    # pq_file_checkpoint = dict({
    #         'finish':True,
    #         'break_row':0 })
    
    # global check_point_file    
    # check_point_file[file_name] = pq_file_checkpoint

    # with open(check_point_file_path, 'wb') as pkl_file:
    #     pickle.dump(check_point_file, pkl_file)
    
    print(f'[ Info ] Callback: Finish {file_name}, save to checkpoint.')

def process_work(info_queue, pq_file_path:str, begin_row:int):
    
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
    
    print(f'[ Info ] Begin export: {pq_file_path}. Rows: {current_file_rows}, begin_row: {begin_row}')
    
    source_file_name = pq_file_path.split('/')[-1]
    for row in tqdm(range(begin_row, current_file_rows)):
        
        # if not stop_work_queue.empty():
        #     stop_work_queue.get(block=False) == quit_work_flag
        #     stop_work_queue.put(stop_work_queue)
        #     raise WorkException(source_file_name, row, ex)
        
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
        
        infos = dict({'source_file_name':source_file_name, 
            'best_image_uid':best_uid, 
            'caption':prompt, 
            'caption_md5':prompt_md5, 
            'image_uid':img_0_uid, 
            'image_md5':img_0_md5, 
            'partner_img_uid':img_1_uid, 
            'partner_img_md5':img_1_md5, 
            'self_better':'true' if best_uid == img_0_uid else 'false'
            })
        try:
            db_helper.insert(infos)
        except Exception as ex:
            break_msg = dict({
                'type':'Break',
                'file_name':source_file_name,
                'rows':current_file_rows,
                'break_row':row
            })
            info_queue.put(break_msg)
            raise WorkException(source_file_name, row, ex)
        
        image_array = np.frombuffer(img_1_bytes, np.uint8) 
        image = cv2.imdecode(image_array, cv2.IMREAD_COLOR) 
        cv2.imwrite(f'./exported_img/{img_1_md5}+{prompt_md5}.png', image)
        
        infos['image_uid'] = img_1_uid
        infos['image_md5'] = img_1_md5
        infos['partner_img_uid'] = img_0_uid
        infos['partner_img_md5'] = img_0_md5
        infos['self_better'] = 'true' if best_uid == img_1_uid else 'false'
        try:
            db_helper.insert(infos)
        except Exception as ex:
            break_msg = dict({
                'type':'Break',
                'file_name:':source_file_name,
                'rows':current_file_rows,
                'break_row':row
            })
            info_queue.put(break_msg)
            raise WorkException(source_file_name, row, ex)
    
    finish_msg = dict({
        'type':'Finish',
        'file_name':source_file_name,
        'rows':current_file_rows
    })
    try:
        info_queue.put(finish_msg)
    except Exception as ex:
        print(ex)
    # print(finish_msg)
    time.sleep(2)
    db_helper.close()
    print(f'[ Info ] Finish export: {pq_file_path}. Rows: {current_file_rows}. Close db_helper.')
    return parquet_file
    
def main(stop_work_queue):

    global check_point_file
    
    if not os.path.exists(img_save_path):
        os.mkdir(img_save_path)
        print(f'[ Warn ] Image save path: {img_save_path} not exists, create one. ')
    
    if os.path.exists(check_point_file_path):
        print(f'[ Info ] Found checkpoint file {check_point_file_path}')
        try:
            with open(check_point_file_path, 'rb') as file:
                check_point_file = pickle.load(file)
        except Exception as ex:
            print(f'[ Error ] Open checkpoint file error: {ex}. Start new work.')
        else:
            print(f'[ Info ] Resume from last work interruption')
            print(f'[ Info ] If you want start a new task, remove/delete {check_point_file_path} and restart the work.')
    else:
        print(f'[ Info ] Start new work.')
            
    print(f'[ Info ] Prepare to run, PC cpu count = {cpu_count()}, so that set Pool.processes = {cpu_count()}. ')

    pool = Pool(cpu_count())
    # pool = Pool(3)
    
    for pq_file in os.listdir(file_root_path):
        if not pq_file.endswith(file_type):
            continue
        
        begin_row = 0

        if pq_file in check_point_file.keys():
            pq_file_checkpoint = check_point_file[pq_file]
            if pq_file_checkpoint['finish'] == True:
                continue
            else:
                begin_row = pq_file_checkpoint['break_row']
        else:
            pq_file_checkpoint =dict({
                'finish':False,
                'break_row':0
            })
            check_point_file[pq_file] = pq_file_checkpoint
        
        # pool.apply_async(func=process_work, args=(file_root_path + pq_file))        
        # pool.apply_async(func=process_work, args=(stop_work_queue, file_root_path + pq_file, begin_row,), 
        #                  callback=finish_callback, error_callback=process_error_callback)
        
        # pool.apply_async(func=process_work, args=(file_root_path + pq_file, begin_row, ),  callback=finish_callback) 
        pool.apply_async(func=process_work, args=(stop_work_queue, file_root_path + pq_file, begin_row, )  , callback=finish_callback) 
    
    pool.close()
    pool.join()
    with open(check_point_file_path, 'wb') as pkl_file:
        pickle.dump(check_point_file, pkl_file)
        
if __name__ == '__main__':    

    check_point_file = dict()

    manager = Manager()
    info_queue = manager.Queue()

    # spy
    spy_process_exit_enent = manager.Event()
    spy_process = Process(target=spy_work, args=(info_queue, spy_process_exit_enent, check_point_file, ))
    spy_process.start()
    
    if not os.path.exists(img_save_path):
        os.mkdir(img_save_path)
        print(f'[ Warn ] Image save path: {img_save_path} not exists, create one. ')
    
    if os.path.exists(check_point_file_path):
        print(f'[ Info ] Found checkpoint file {check_point_file_path}')
        try:
            with open(check_point_file_path, 'rb') as file:
                check_point_file = pickle.load(file)
        except Exception as ex:
            print(f'[ Error ] Open checkpoint file error: {ex}. Start new work.')
        else:
            print(f'[ Info ] Resume from last work interruption:\n')
            print(check_point_file)
            print(f'[ Info ] If you want start a new task, remove/delete {check_point_file_path} and restart the work.')
    else:
        print(f'[ Info ] Start new work.')
    
    # spy_process_exit_enent.set()
    # exit(0)

    print(f'[ Info ] Prepare to run, PC cpu count = {cpu_count()}, so that set Pool.processes = {cpu_count() -1 }, and spy_process = 1. ')

    pool = Pool(cpu_count() - 1)
    # pool = Pool(3)
    
    for pq_file in os.listdir(file_root_path):
        if not pq_file.endswith(file_type):
            continue
        
        begin_row = 0

        if pq_file in check_point_file.keys():
            pq_file_checkpoint = check_point_file[pq_file]
            if pq_file_checkpoint['finish'] == True:
                continue
            else:
                begin_row = pq_file_checkpoint['break_row']
        else:
            pq_file_checkpoint =dict({
                'finish':False,
                'break_row':0
            })
            check_point_file[pq_file] = pq_file_checkpoint
        
        # pool.apply_async(func=process_work, args=(file_root_path + pq_file))        
        # pool.apply_async(func=process_work, args=(stop_work_queue, file_root_path + pq_file, begin_row,), 
        #                  callback=finish_callback, error_callback=process_error_callback)
        
        # pool.apply_async(func=process_work, args=(file_root_path + pq_file, begin_row, ),  callback=finish_callback) 
        pool.apply_async(func=process_work, args=(info_queue, file_root_path + pq_file, begin_row, ), callback=finish_callback) 
    
    pool.close()
    pool.join()

    # print(check_point_file)

    # with open(check_point_file_path, 'wb') as pkl_file:
    #     pickle.dump(check_point_file, pkl_file)
    
    spy_process_exit_enent.set()
    spy_process.join()
    print('Done!')
        