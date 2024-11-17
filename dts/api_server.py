from flask import Flask, request
import json
import threading
import db_helper
import random
import datetime

app = Flask(__name__)

# session id
session_id = ""

# 密码本
mmb = "1@wdvfr567yh=bgt8ijnmku09-."
# 携带访问，简单身份认证
auth_token = 'asgdae4wgawhgaertgwhdnhjexssarx'
# 一次返回的长度
batch_size = 200
# 访问计数器
req_counter = 0
# 错误退出list
error_bye_dict = {}

# 所有图片的，保存路径
all_imgs = []
begin_index = 0

# client_auth_set
client_auth_dict = dict({})


@app.route('/dts/hi', methods=['GET'])
def register():
    t = request.args.get("t")
    cid = request.args.get("cid")
    work_space = request.args.get("ws")
    cuda_name = request.args.get("cudan")

    # print(f"cid:[{cid}]")
    if t != auth_token:
        return ""
    if cid == "" or cid == None:        
        return ""

    if cid in client_auth_dict.keys():
        return ""
    
    if work_space == "" or work_space == None:
        return ""
    if cuda_name == "" or cuda_name == None:
        return ""
    
    rand_client_psd = "".join(random.choices(mmb, k=10))
    client_auth_dict[cid]={'sid':session_id, 'work_space':work_space,'cuda_name': cuda_name,
                           'register_time':datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'req_counter':0, 'cpsd':rand_client_psd, 'req_time':[]}
    return {'at':rand_client_psd, 'sid':session_id}

'''
{
    "last": -1
}
'''
@app.route('/dts/done', methods=['GET'])
def is_done():
    t = request.args.get("t")
    cid = request.args.get("cid")
    sid = request.args.get("sid")

    # print(f"t:[{t}], cid:[{cid}]")
    if (cid not in client_auth_dict.keys()):
        return ""
    if client_auth_dict[cid]['cpsd'] != t: 
        return ""
    if (sid == None) or (sid!=session_id):
        return ""
    return json.dumps({'last':len(all_imgs) - begin_index})

'''
return sample:
{"items":["", "", ""]}
'''
@app.route('/dts/gimgs', methods=['GET'])
def g_imgs():
    t = request.args.get("t")
    cid = request.args.get("cid")
    sid = request.args.get("sid")

    if (cid not in client_auth_dict.keys()):
        return ""
    if client_auth_dict[cid]['cpsd'] != t: 
        return ""
    if (sid == None) or (sid!=session_id):
        return ""
      
    res = ""
    with threading.Lock():

        global begin_index
        global req_counter

        if begin_index >= len(all_imgs):
            return json.dumps({"items":[]})
                
        req_counter = req_counter + 1
        client_auth_dict[cid]['req_counter'] = client_auth_dict[cid]['req_counter'] + 1
        temp_list = client_auth_dict[cid]['req_time']
        temp_list.append(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        client_auth_dict[cid]['req_time'] = temp_list[-5:]

        end_index = min(len(all_imgs), begin_index + batch_size)
        res = json.dumps({"items":all_imgs[begin_index:end_index]})
        if begin_index + batch_size >= len(all_imgs):
            begin_index = len(all_imgs)
        else:
            begin_index = begin_index + batch_size
    print(client_auth_dict)
    return res

@app.route('/dts/say_yes', methods=['GET'])
def say_yes():
    detail_dict = {}
    dk = ['sid', 'work_space', 'cuda_name', 'register_time', 'req_counter']
    for k, v in client_auth_dict.items():
        temp_d = {}
        for kk in v.keys():
            if kk in dk:
                temp_d[kk] = v[kk]
        detail_dict[k] = temp_d
    
    # print(detail_dict)

    info_dict = {
        'current:all': f'{begin_index}:{len(all_imgs)}',
        'register_number':len(client_auth_dict.keys()),
        'work_name':[i for i in client_auth_dict.keys()],
        'error_bye':error_bye_dict,
        'detail':detail_dict
    }
    return json.dumps(info_dict)

@app.route('/dts/say_bye', methods=['GET'])
def error_quit():
    global error_bye_dict
    machine_x = request.args.get("mx")
    device_x = request.args.get("dx")
    cid = request.args.get("cid")
    error_bye_dict[cid]={'machine_x':machine_x, 'device_x':device_x}
    return json.dumps(error_bye_dict)


def init():
    global all_imgs
    all_imgs = db_helper.select_imgs_by_score()
    print(f"[ Info ]: all_imgs length: {len(all_imgs)}")


if __name__ == '__main__':
    session_id = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    init()

    app.run(debug=True, port=19972)
