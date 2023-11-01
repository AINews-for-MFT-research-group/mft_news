#Get NER results from Zhipuai

import pandas as pd

import json
import zhipuai
import time
import config

zhipuai.api_key = config.API_KEY


tar_file = "result/english/%d.json"

result_file = "result/english_item/%d.json"

with open('prompt_getitem.txt') as f:
    prompt = f.read()


def send_req(tar_text):
    while True:
        response = zhipuai.model_api.async_invoke(
            model="chatglm_std",
            prompt=[{"role": "user", "content": prompt % tar_text}],
            top_p=0.7,
            temperature=0.9,
        )
        print(response)
        if response['code'] == 200:
            return response['data']['task_id']
        if response['code'] != 1302:
            return None    
        else:
            print('wait')
        time.sleep(10)
    
def query_status(task_id):
    response = zhipuai.model_api.query_async_invoke_result(task_id)
    if response['success']:
        if response['data']['task_status'] == 'SUCCESS':
            return {'status': 1, 'content': response['data']['choices'][0]['content']}
        else:
            return {'status':0}
    else:
        return {'status': -1}

def check_wait(wait_list): 
    tmp_list = []
    for item in wait_list:
        tmp_res = query_status(item[0])
        if tmp_res['status'] == 0:
            tmp_list.append(item)
        
        if tmp_res['status'] == 1:
            item[1]['item_res'] = tmp_res['content']
            with open(result_file % item[1]['content_id'],'w') as f:
                json.dump(item[1],f,indent=6,ensure_ascii=False) 
            print('get trans %d' % item[1]['content_id'])
        
        if tmp_res['status'] == -1:
            print('trans error %d' % item[1]['content_id'])
        time.sleep(2)
    return tmp_list 




item_list = pd.read_csv('result/english_data.csv')


WAIT_NUM = 5
wait_list = []


for index, row in item_list.iterrows():
    print(index)
    with open(tar_file % row['content_id']) as f:
        cur_item = json.load(f)
    tmp_id = send_req(cur_item['content'])
    
    
    if tmp_id:
        wait_list.append([tmp_id,cur_item])
    else:
        print('trans error')

    time.sleep(3)

    while len(wait_list) >= WAIT_NUM:
        print('check_list %d' % len(wait_list))
        time.sleep(5)
        wait_list = check_wait(wait_list)
    
print('final check ----------')

while len(wait_list) > 0:
    print('check_list %d' % len(wait_list))
    wait_list = check_wait(wait_list)
    time.sleep(5)   
    
