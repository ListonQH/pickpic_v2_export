import json
import requests
import random
import time

auth_token = 'asgdae4wgawhgaertgwhdnhjexssarx'
# 任何请求的 最大 尝试 次数
max_try = 10

hi_url = 'http://127.0.0.1:19972/dts/hi'
last_url = 'http://127.0.0.1:19972/dts/done'
gimgs_url = 'http://127.0.0.1:19972/dts/gimgs'

# hi_url = 'https://www.grjywlqh.cn/dts/hi'
# last_url = 'https://www.grjywlqh.cn/dts/done'
# gimgs_url = 'https://www.grjywlqh.cn/dts/gimgs'

cid = "".join(random.choices(auth_token, k=15))
access_token = ""
sid = ""
try_num = 0
while True:
    try_num = try_num + 1
    res = requests.get(hi_url, params={'t':auth_token, 'cid':cid, 'ws':'ubuntu', 'cudan':-11})
    if res.ok:
        res_text_dict = json.loads(res.text)
        access_token = res_text_dict['at']
        sid = res_text_dict['sid']
        print(f"res_text_dict:[{res_text_dict}]")
        break
    if try_num > max_try:
        print("Error: can't get access token")
        exit(0)

try_num = 0
while True:
    if try_num > max_try:
        break
    res = requests.get(last_url, params={'t':access_token, 'cid':cid, 'sid':sid})
    if not res.ok:
        time.sleep(3)
        try_num = try_num + 1
        continue
    print(f'res.text:[{res.text}]')
    if json.loads(res.text)['last'] <= 0:
        try_num = -1
        break

    res = requests.get(gimgs_url, params={'t':access_token, 'cid':cid, 'sid':sid})
    if not res.ok:
        time.sleep(3)
        try_num = try_num + 1
        continue
    work_list = list(json.loads(res.text)['items'])
    if len(work_list) == 0:
        try_num = -2
        break
    for i in work_list:
        print(f'Qwen2-7B: [{i}]')
        time.sleep(0.2)

