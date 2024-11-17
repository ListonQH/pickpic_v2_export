import os
import numpy
import hashlib               #导入功能模块，此模块有MD5,SHA1,SHA256等方法

def get_img_file_md5(img_path:str):
    if os.path.exists(img_path):
        f = open(img_path, 'rb')
        md5_value = hashlib.md5(str).hexdigest()
        return md5_value
    return ""

def get_np_array_md5(data_arr:numpy.ndarray):
    arr = data_arr.to_pybytes()
    return hashlib.md5(arr).hexdigest()

def get_string_md5(str:str):
    return hashlib.md5(str.encode()).hexdigest()

if __name__ == '__main__':
    print(get_string_md5('123'))