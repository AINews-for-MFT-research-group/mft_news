
# Page content crawler

import config
from sqlalchemy import Column, String, create_engine, Integer, Text, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
import time
import threading

from goose3 import Goose 
from goose3.text import StopWordsChinese, StopWordsArabic, StopWordsKorean


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
    #to_page = relationship("PageResult", backref = "to_task")
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


# 初始化数据库连接:
engine = create_engine(config.SQLALCHEMY_DATABASE_URI)

Base.metadata.create_all(engine)

# 创建DBSession类型:
DBSession = sessionmaker(bind=engine)

BATCH_NUM = 5
USER_HEAD = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36'

RUNNING_FLAG = True


def get_page():
    global RUNNING_FLAG
    print('thread start')
    session = DBSession()
    task_list=  session.query(PageResult).filter(PageResult.content_status == 0).limit(BATCH_NUM)
    if task_list:
        #print('before')
        update_list = []
        for item in task_list:
            #print(item.url)
            item.content_status = 1
            update_list.append({'id':item.id, 'url': item.url, 'language': item.language})
        session.commit()
        #print('after') 
        for item in update_list:
            cur_url = item['url']
            cur_lang = item['language']
            g = None
            if "Chinese" in cur_lang:
                g =  Goose({'browser_user_agent': USER_HEAD,
                            'stopwords_class': StopWordsChinese})
                print('Chinese')
            else:
                if "Korean" in cur_lang:
                    g =  Goose({'browser_user_agent': USER_HEAD,
                                'stopwords_class': StopWordsKorean})
                    print('Korean')
                else:
                    if "Arabic" in cur_lang:
                        g =  Goose({'browser_user_agent': USER_HEAD,
                                'stopwords_class': StopWordsArabic})
                        print('Arabic')
                    else:
                        g = Goose({'browser_user_agent': USER_HEAD})
                        print('Normal')
            try:
                print(item['id'])
                print(cur_url)
                article = g.extract(url=cur_url)
                item['content'] = article.cleaned_text
                item['title'] = article.title[:200]
                item['keywords'] = article.meta_keywords[:200]
                item['content_status'] = 2
            except Exception as e:
                print(e)
        session.bulk_update_mappings(PageResult, update_list)
        session.commit()
    else:
        RUNNING_FLAG = False   
    session.close()
        
    


THRED_NUM = 5

thread_list = []
for i in range(THRED_NUM):
    t = threading.Thread(target=get_page)
    t.start()
    thread_list.append(t)
    time.sleep(5)



while True:
    tmp_list = []
    for cur_t in thread_list:
        if cur_t.is_alive():
            tmp_list.append(cur_t)
    thread_list = tmp_list
    if RUNNING_FLAG:       
        while len(thread_list) < THRED_NUM:
            t = threading.Thread(target=get_page)
            t.start()
            thread_list.append(t)
            time.sleep(5)
    else:
        if len(thread_list) == 0:
            break
    time.sleep(5)


            
            

                    
            
            
            
            
        
        
    