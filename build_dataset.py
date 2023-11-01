#Build datasets based on the MFT Dictionaries.

import config
from sqlalchemy import Column, String, create_engine, Integer, Text, ForeignKey, or_
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.expression import func
import time
import pandas as pd
import json
# 创建对象的基类:
Base = declarative_base()

class PageResult(Base):
    __tablename__ = 'page_result'
    id = Column(Integer, primary_key=True, autoincrement=True)
    content = Column(Text)
    keywords = Column(String(200))
    title = Column(String(200))
    url = Column(String(200))
    language = Column(String(20))
    content_status = Column(Integer)
    ai_status = Column(Integer)
    finishedat = Column(Integer)
    def __init__(self, url, language):
        self.language = language
        self.url = url
        self.content_status = 0
        self.ai_status = 0
        self.finishedat = 0



class Task(Base):
    # 表的名字:
    __tablename__ = 'task'
    # 表的结构:
    id = Column(Integer, primary_key=True, autoincrement=True)
    ori_id = Column(Integer)
    title = Column(String(200))
    opening = Column(String(200))
    url = Column(String(200))
    date = Column(Integer)
    task_name = Column(String(50))
    language = Column(String(20))
    content_id = Column(Integer, ForeignKey("page_result.id"))
    def __init__(self, tar_id, tar_title, tar_opening, tar_url, tar_date, tar_task, tar_language, tar_content):
        self.ori_id = tar_id
        self.title = tar_title
        self.opening = tar_opening
        self.url = tar_url
        self.date = tar_date
        self.task_name = tar_task
        self.language = tar_language
        self.content_id = tar_content

def date2ts(tar_date):
    timeArray = time.strptime(tar_date, "%d-%b-%Y %I:%M%p")
    return(int(time.mktime(timeArray)))

def ts2date(tar_ts):
    return time.strftime("%Y-%m-%d %H:%M", time.localtime(tar_ts))


# 初始化数据库连接:
engine = create_engine(config.SQLALCHEMY_DATABASE_URI)

Base.metadata.create_all(engine)

# 创建DBSession类型:
DBSession = sessionmaker(bind=engine)

session = DBSession()



en_df = pd.read_csv('result/chinese_result.csv')
en_df = en_df.sort_values('word_num',ascending=False, ignore_index=True)

result_list = []

for index, row in en_df.iloc[:2500].iterrows():
    print(index)
    page_item = session.get(PageResult, row['id'])
    task_list = session.query(Task).filter(Task.content_id == row['id']).all()
    task_item = task_list[0]
    task_date = ts2date(task_item.date)
    tmp_item = {
        "ori_id": int(task_item.ori_id),
        "content_id": int(row['id']),
        "mft_num": int(row['word_num']),
        "date": task_date,
        "language": page_item.language,
        "title": task_item.title,
        "url": page_item.url 
    }
    result_list.append(tmp_item)
    tmp_json = tmp_item.copy()
    tmp_json['content'] = page_item.content
    with open('result/chinese/%d.json' % row['id'],'w') as f:
        json.dump(tmp_json,f,indent=6,ensure_ascii=False)
result_pd = pd.DataFrame(result_list)
result_pd.to_csv('result/chinese_data.csv')

