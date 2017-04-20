# -*- coding: utf-8 -*-
"""
Created on Fri Apr  7 19:34:48 2017

@author: naoya
"""

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
import pandas as pd
from bs4 import BeautifulSoup
import time
import os,sys
import csv
import glob
from datetime import datetime as dt
from concurrent.futures import ThreadPoolExecutor

#更新情報csv作成関数
def write_crowling_update(company='企業名',up_time_1='クローリン更新コメント時間_最新',up_time_2='クローリン更新コメント時間_最古',re_time='クローリング参照コメント時間',check_result='クローリング更新結果',file_name='/クローリング更新情報.csv',w_a='w'):
    result = open(result_path + file_name , w_a, newline='', encoding='utf-8')
    csv_writer = csv.writer(result)
    csv_writer.writerow([company,up_time_1,up_time_2,re_time,check_result])
    result.close()
    
#一覧ページの日付の最初と最後を取得して返す
def page_date_get(store,now,pref_url):
    #トップページの日付確認
    try:
        check_table = store.find('table',attrs={'class','md_table tline'}).find_all('td',attrs={'class','date'})
        top_date_tmp = check_table[0].text
        bottom_date = check_table[-1].text
        top_date_tmp = top_date_tmp.replace('今日', '/'.join(now.split('-')[0:3]))
        bottom_date = bottom_date.replace('今日', '/'.join(now.split('-')[0:3]))
        try:
            top_date_tmp = dt.strptime(top_date_tmp,'%Y/%m/%d %H:%M')
        except:
            top_date_tmp = dt.strptime(top_date_tmp,'%Y/%m/%d')
        try:
            bottom_date = dt.strptime(bottom_date,'%Y/%m/%d %H:%M')
        except:
            bottom_date = dt.strptime(bottom_date,'%Y/%m/%d')
    except:
        top_date_tmp = 'Not found comment in ' + str(pref_url)
    return top_date_tmp,bottom_date
        
            
#個人のページ取得用関数
def get_individual_page(store,driver2,comment_count):
    for forecast in store.find('table',attrs={'class','md_table tline'}).find_all('tr'):
        forecast = forecast.find('a')
        comment_page_id = str(comment_count)
        comment = 'http://minkabu.jp' + forecast.get("href")
        driver2.get(comment)
        store = BeautifulSoup(driver2.page_source, 'lxml')
        with open('comment/{}.html'.format(comment_page_id), 'wb') as f:
            f.write(store.encode('utf-8'))
        print(comment_page_id + '件目です。')
        time.sleep(0.5)
        comment_count += 1
    return comment_count,comment_page_id

#各企業ごとにクローリングしていく関数
def sub_main(code):
    #現在作業しようとしている企業関係のPathの準備
    code_path = saving_path + '/' + str(code[0]) + '-' + code[1]
    print(code[1])
    #この企業のコメントが前回は正しくクローリングされているか
    try:
        result_num = name_list.index(code[1])
        decision = True
    except:
        result_num = 'NotNum'
        decision = False
    if(decision):
        #ある場合は前回の更新の一番新しい時間を取得　前回更新をしていない場合は、前回の参照の時間を取得
        if(newest_csv.iloc[result_num,1] != 'Not update' and newest_csv.iloc[result_num,1].find('Not found') == -1):
            last_date = dt.strptime(newest_csv.iloc[result_num,1],'%Y-%m-%d %H:%M:%S')
        else:
            last_date = dt.strptime(newest_csv.iloc[result_num,3],'%Y-%m-%d %H:%M:%S')
    else:
        #ない場合は仮の年月を代入
        last_date = dt.strptime('1930/04/01', '%Y/%m/%d')
    #企業のhtmlフォルダ作成
    print(last_date)
    try:
        os.mkdir(code_path)
    except:
        pass
    #現在時刻を扱いやすい形に変形
    now = str(dt.today()).replace(' ','-').split('.')[0].replace(':','-')
    pref_url = 'http://minkabu.jp/stock/' + str(code[0]) + '/pick'
    #二つのブラウザ準備
    driver = webdriver.PhantomJS()
    driver2 = webdriver.PhantomJS()
    #売買予想の一覧トップページに移動
    driver.get(pref_url)
    store = BeautifulSoup(driver.page_source, 'lxml')
    top_date_tmp = []
    bottom_date = []
    top_date_tmp,bottom_date = page_date_get(store,now,pref_url)
    print(top_date_tmp)
    print(last_date)
    #万が一ページが存在しなければこれ以上はなにもできないのでストップ 特に更新がない場合も、この企業のクローリングは行わない
    try:
        top_date_tmp.find('Not found comment')
        write_crowling_update(code[1],top_date_tmp,'Not found',last_date,'Not found',w_a='a')
        driver.quit()
        driver2.quit()
        return 0
    except:
        pass
    if(top_date_tmp <= last_date):
        write_crowling_update(code[1],'Not update','Not update',last_date,'Not update',w_a='a')
        driver.quit()
        driver2.quit()
        return
    #今回のフォルダ作成 移動
    top_date = top_date_tmp
    os.mkdir(code_path + '/' + now)
    os.mkdir(code_path + '/' + now + '/' + 'comment')
    os.chdir(code_path + '/' + now)
    count = 1
    time.sleep(0.5)
    page_id = str(count)
    comment_count = 1
    print('1ページ目です。')
    with open('{}.html'.format(page_id), 'wb') as f:
        f.write(store.encode('utf-8'))
    comment_count,comment_page_id = get_individual_page(store,driver2,comment_count)
    while True:
        count += 1
        page_id = str(count)
        try:
            driver.find_element_by_link_text('次へ »').click()
            time.sleep(0.5)
            store = BeautifulSoup(driver.page_source, 'lxml')
            top_date_tmp,bottom_date = page_date_get(store,now,pref_url)
            #このページで新しいページがなければ、この企業のクローリングは終了
            if(top_date_tmp < last_date):
                write_crowling_update(code[1],top_date,bottom_date,last_date,str(top_date > last_date and top_date_tmp < last_date and bottom_date <= last_date),w_a='a')
                driver.quit()
                driver2.quit()
                break
            print(page_id + 'ページ目です。')
            with open('{}.html'.format(page_id), 'wb') as f:
                f.write(store.encode('utf-8'))
            comment_count,comment_page_id = get_individual_page(store,driver2,comment_count)
        except:
            if(top_date_tmp> last_date):
                #ここに来た場合は、初めての更新のはず
                write_crowling_update(code[1],top_date,bottom_date,last_date,'First',w_a='a')
                driver.quit()
                driver2.quit()
            break



# プログラムの場所, HTMLの保存ディレクトリ,
# スクレイピング結果保存ディレクトリ
running_path = os.getcwd()
saving_path = running_path + r'/Output'
result_path = running_path + r'/result'

data_value_list = []
page_id_list = []

# HTML用ディレクトリがなければ生成
try:
    os.mkdir(saving_path)
except:
    pass

#同様にcsv用ディレクトリがなければ生成
try:
    os.mkdir(result_path)
except:
    pass

#日経225企業コードのリスト取得
df= pd.read_csv("nikkei225-stock-prices.txt",encoding="utf-8",delimiter='\t')
code_list = df.iloc[:,:2]
code_count = 0


#前回の更新情報csvの中身取得 ない場合はこの時点でファイルを作成
try:
    newest_csv = pd.read_csv(result_path + '/クローリング更新情報.csv', encoding = 'utf-8', engine='python')
    newest_csv.to_csv(result_path + '/クローリング更新情報_前回.csv',encoding='utf-8',index = False)
except:
    write_crowling_update()
    newest_csv = pd.read_csv(result_path + '/クローリング更新情報.csv', encoding = 'utf-8', engine='python')
    
name_list = list(newest_csv.iloc[:,0])
#更新情報csvを新しく準備
write_crowling_update()

executer = ThreadPoolExecutor(max_workers=3)

for code in code_list.values: #各企業毎にクローリング開始
    executer.submit(sub_main,code)



