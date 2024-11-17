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
from util_exception import WorkException, SQLException


check_point_file_path = './data_export.checkpoint'
file_root_path = './parquet/'
file_type = '.parquet'    
img_save_path = './exported_img/'

# check_point_file = dict()

def spy_work(queue, spy_process_exit_event, check_point_file):
# def spy_work(queue, spy_process_exit_event):    
    print('[ Inof ] Spy process start ...')
    
    # global check_point_file
    print('[ Info ] spy_work print( check_point_file ):')
    print(check_point_file)
    
    ctrl_c_flag = False
    
    while True:
        try:
            if not queue.empty():
                msg:dict = queue.get(block=False)
                
                if msg['type'] == 'Break':
                    msg['finish'] = False
                elif msg['type'] == 'Finish':
                    msg['finish'] = True
                
                source_file_name = msg['file_name']

                check_point_file[source_file_name]=msg
                # print('spy: \n', check_point_file)
                with open(check_point_file_path, 'wb') as pkl_file:
                    pickle.dump(check_point_file, pkl_file)
            
            if queue.empty() and spy_process_exit_event.is_set():
                break         
            if not ctrl_c_flag:
                time.sleep(1)
        except KeyboardInterrupt:
            ctrl_c_flag = True
            print('[ Info ] Spy process Ctrl + C ...')        
    
    print('[ Info ] Spy process stop ...')

def process_error_callback(ex:WorkException):    
    print(f'[ Error ] Callback: Stop {ex.file_name} export, at row: {ex.break_row}.')

def process_finish_callback(file_name:str):    
    print(f'[ Info ] Callback: Finish {file_name}, save to checkpoint.')

def export_process(export_process_exit_event, info_queue, pq_file_path:str, begin_row:int):
    try:    
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

            # test error_callback
            # if row == 100:
            #     break_msg = dict({
            #         'type':'Break',
            #         'file_name:':source_file_name,
            #         'rows':current_file_rows,
            #         'break_row':row
            #     })
            #     info_queue.put(break_msg)
            #     raise WorkException(source_file_name, row, 'ex')
            
            if export_process_exit_event.is_set():
                break_msg = dict({
                    'type':'Break',
                    'file_name:':source_file_name,
                    'rows':current_file_rows,
                    'break_row':row
                })
                info_queue.put(break_msg)
                return source_file_name
            
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
        
    # except KeyboardInterrupt:        
    #     raise WorkException(source_file_name, row, 'KeyboardInterrupt')
    # except SQLException:
    #     raise WorkException(source_file_name, row, 'SQLError')
    except:
        break_msg = dict({
                    'type':'Break',
                    'file_name':source_file_name,
                    'rows':current_file_rows,
                    'break_row':row
                })
        info_queue.put(break_msg) 
        raise WorkException(source_file_name, row, 'Error')
              
    try:
        finish_msg = dict({
                'type':'Finish',
                'file_name':source_file_name,
                'rows':current_file_rows
            })
        info_queue.put(finish_msg)
        time.sleep(2)
        db_helper.close()    
    except:
        info_queue.put(finish_msg)
        print('[ Error ] Unknown error occurs !')
    print(f'[ Info ] Finish export: {pq_file_path}. Rows: {current_file_rows}. Close db_helper.')
    return source_file_name
         
if __name__ == '__main__':    

    check_point_file = dict()

    manager = Manager()
    info_queue = manager.Queue()

    # export 
    export_process_exit_event = manager.Event()
       
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
            print()
            print(f'[ Info ] If you want start a new task, remove/delete {check_point_file_path} and restart the work.')
    else:
        print(f'[ Info ] Start new work.')
    
    # spy
    spy_process_exit_event = manager.Event()
    spy_process = Process(target=spy_work, args=(info_queue, spy_process_exit_event, check_point_file, ))
    # spy_process = Process(target=spy_work, args=(info_queue, spy_process_exit_event,  ))
    spy_process.start()
     
    # spy_process_exit_event.set()
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
        
        pool.apply_async(func=export_process, args=(export_process_exit_event, info_queue, file_root_path + pq_file, begin_row, ), 
                         callback=process_finish_callback, error_callback=process_error_callback) 
        # pool.apply_async(func=export_process, 
        #                  args=(export_process_exit_event, info_queue, file_root_path + pq_file, begin_row, ))
    
    try:
        pool.close()
        pool.join()
    except KeyboardInterrupt as ex:
        print('[ Warn ] Press Ctrl + C, stop all processes...')
    else:
        print('[ Info ] All process work finish. Save info and exit. ')
    finally:
        export_process_exit_event.set()

    spy_process_exit_event.set()
    export_process_exit_event.set()
    pool.join()

    try:
        spy_process.join()
    except :
        print('Main except 22')
    print('[ Info ] Done!')
        