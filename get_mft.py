# Get Mft results from Zhipuai


import pandas as pd

import json
import zhipuai
import time
import config 

zhipuai.api_key = config.API_KEY

with open('prompt_mft_old.txt') as f:
    prompt = f.read()

item_list = pd.read_csv('result/english_data.csv')

input_file = "result/english_item/%d.json"
output_file = "result/english_mft/%d.json"


TAR_CAT = ['政府','企业','个人']



def send_req(tar_list):
    content = ""
    for i in range(len(tar_list)):
        if tar_list[i]['category'] in TAR_CAT:
            content += "文本%d:\n%s\n\n" % (i+1, tar_list[i]['related-info'])
    
    if len(content) == 0:
        return None
        
    content = prompt % content
    
    while True:
        response = zhipuai.model_api.async_invoke(
            model="chatglm_std",
            prompt=[{"role": "user", "content": content}],
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
            item[1]['mft_res_old'] = tmp_res['content']
            with open(output_file % item[1]['content_id'],'w') as f:
                json.dump(item,f,indent=4,ensure_ascii=False) 
            print('get trans %d' % item[1]['content_id'])
            
        if tmp_res['status'] == -1:
            print('trans error %d' % item['id'])
        time.sleep(2)
    return tmp_list 


WAIT_NUM = 5
wait_list = []

for index, row in item_list.iloc[310:2500].iterrows():
    print(index)
    with open(input_file % row['content_id']) as f:
        cur_item = json.load(f)
    try:
        cur_entity = json.loads(cur_item['item_res'])
        cur_entity = json.loads(cur_entity)
    except Exception as e:
        print(e)
        #print(index)
        #print(cur_item['item_res'])
        with open('miss.txt','a') as f:
            f.write(str(index)+'\n')
        print('json error')
        continue
    tmp_id = send_req(cur_entity)
    if tmp_id:
        wait_list.append([tmp_id,cur_item])
    else:
        print('trans error')
    time.sleep(3)
    while len(wait_list) >= WAIT_NUM:
        #if time.time() - last_check_ts > CHECK_INTERVAL:
        print('check_list %d' % len(wait_list))
        time.sleep(5)
        wait_list = check_wait(wait_list)

print('final check ----------')

while len(wait_list) > 0:
    print('check_list %d' % len(wait_list))
    wait_list = check_wait(wait_list)
    time.sleep(5)
       
