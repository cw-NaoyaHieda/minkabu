# -*- coding: utf-8 -*-
"""
Created on Sun Apr  9 19:38:53 2017

@author: naoya
"""

import pandas as pd
from bs4 import BeautifulSoup
import time
import os
import csv
import codecs
import glob
import numpy as np
from datetime import datetime as dt

#スクレイピング用の関数



#更新情報記入用関数
def write_scraping_update(company='企業名',up_time_1='スクレイピング更新コメント時間_最新',up_time_2='スクレイピング更新コメント時間_最古',re_time='スクレイピング参照コメント時間',last_folder = '前回取得した最新フォルダ',check_result='スクレイピング更新結果',file_name='/スクレイピング更新情報.csv',w_a='w'):
    result = open(result_path + file_name , w_a, newline='', encoding='utf-8')
    csv_writer = csv.writer(result)
    csv_writer.writerow([company,up_time_1,up_time_2,re_time,last_folder,check_result])
    result.close()

#今回取得する可能性のあるコメントを全て取得
def get_comment(date_path_list):
    comment_path_list = []
    comment_list = []
    for date_path in date_path_list:
        for path in glob.glob(date_path + '/comment/*.html'):
            comment_path_list.append(path) 
    for comment_path in comment_path_list:
        comment_list.append(get_detail(comment_path))
    return comment_list

#各ページの詳細を取得
def get_detail(html_path):
    
    f = codecs.open(html_path,'r','utf-8')
    store = BeautifulSoup(f, 'lxml')
    f.close()
    
    
    table = store.find('div',attrs={'class','pick-show'})
    content = store.find_all('meta')[1].get('content')
    try:
        date_time = table.find('div',attrs={'class','ly_col ly_colsize_7'}).text
        index_1 = date_time.find('：')
        index_2 = date_time.find('(')
        date = date_time[index_1+1:index_2]
        fore_time = date_time[index_2+1:-2]
    except:
        date = None
        fore_time = None
    try:
        kaiuri = table.find('div',attrs={'class','md_card_ti'}).find(('div')).text
    except:
        kaiuri = None
    try:
        peoplt_text = table.find('div',attrs={'class','md_contribute_img_box'}).find_all('a')[1].text
        index_1 = peoplt_text.find('(')
        people = peoplt_text[:index_1][:-1]
    except:
        people = None
    try:
        point_text = table.find_all('span',attrs={'class',''})[-1].text
        index_2 = point_text.find('point')
        point = point_text[1:index_2]
    except:
        point = None
    try:
        kabuka_first_text = table.find('span',attrs={'class','ly_col ly_colsize_4'}).text
        index_1 = kabuka_first_text.find('：')
        index_2 = kabuka_first_text.find('円')
        kabuka_first = kabuka_first_text[index_1+1:index_2]
    except:
        kabuka_first = None
    try:
        get_point = table.find('span',attrs={'class','fclbl fsl'}).text[:-3]                            
    except:
        try:
            get_point = table.find('span',attrs={'class','fcrd fsl'}).text[:-3]
        except:
            get_point = None
    try:
        detail = table.find('table',attrs={'class','md_table bgNone'})
   
    except:
        detail = None
    try:
        target_price = detail.find('tr',attrs={'class','target_price'}).find('td',attrs={'class','cell fwb'}).text[:-1]                
    except:
        target_price = None
    try:
        period = detail.find('tr',attrs={'class','target_time'}).find('td',attrs={'class','cell fwb'}).text
    except:
        period = None
    try:
        divided = detail.find('tr',attrs={'class','reason'}).find('td',attrs={'class','cell fwb'}).text
    except:
        divided = None
    try:
        comment = store.find('div',attrs={'class','picks_text'}).text
    except:
        comment = None
    try:
        stock_price = store.find('span',attrs={'class','stock_price'}).text
    except:
        stock_price = None
    try:
        now_text = store.find('span',attrs={'class','cur fsm'}).text
        index_1 = now_text.find('日')
        index_2 = now_text.find('現')
        now_date = now_text[:index_1+1]
        now_time = now_text[index_1+2:index_2-1]
    except:
        now_date = (None)
        now_time = (None)
    check_get = store.find('div',attrs={'class','md_flashNotice theme_notice'})
    
    if(check_get is None and date is not None):
        return list([content,company_name,date,fore_time,kaiuri,people,point,kabuka_first,get_point
                ,target_price,period,divided,comment,stock_price,now_date,now_time,html_path])
    
    try:
        check_get.text.find('一致するデータが')
        return list(['This page is not found',company_name,'1880/1/1','0:00',kaiuri,people,point,kabuka_first,get_point
                ,target_price,period,divided,comment,stock_price,now_date,now_time,html_path])
    except:
        return list(['Reason unknown',company_name,'1885/1/1','0:00',kaiuri,people,point,kabuka_first,get_point
                ,target_price,period,divided,comment,stock_price,now_date,now_time,html_path])
    

def write_scraping_line(file_name,company='企業',content='概要',date='予想日',fore_time='予想時刻',kaiuri='売買',people='ユーザ名',point='所持ポイント',kabuka_first='登録時株価',get_point='取得ポイント',target_price='目標株価',period='予想期間',divided='理由',comment='コメント',stock_price='データ取得時株価',now_date='データ取得日',now_time='データ取得時間',file_list='参照html'):
    result = open(result_path + '/' + filename + '.csv', 'a', newline='', encoding='utf-8')
    csv_writer = csv.writer(result)
    csv_writer.writerow([content,company,date,fore_time,kaiuri,people,point,
                     kabuka_first,get_point,target_price,period,divided,
                     comment,stock_price,now_date,now_time,file_list])
    result.close()



# プログラムの場所, HTMLの保存ディレクトリ,
# スクレイピング結果保存ディレクトリ
running_path = os.getcwd()
saving_path = running_path + '\\Output'
result_path = running_path + '\\result'

# HTML用・結果用ディレクトリがなければ生成
try:
    os.mkdir(saving_path)
except OSError:
    pass

#同様にcsv用ディレクトリがなければ生成
try:
    os.mkdir(result_path)
except OSError:
    pass

#スクレイピングの更新情報を記録するcsvの取得と準備
try:
    newest_csv = pd.read_csv(result_path + '/スクレイピング更新情報.csv', encoding = 'utf-8', engine='python')
    newest_csv.to_csv(result_path + '/スクレイピング更新情報_前回.csv',encoding='utf-8',index = False)
except:
    write_scraping_update()
    newest_csv = pd.read_csv(result_path + '/スクレイピング更新情報.csv', encoding = 'utf-8', engine='python')

#記録のある企業の名前を取得
name_list = list(newest_csv.iloc[:,0])
#更新情報csvを新しく準備
write_scraping_update()

#htmlのフォルダがある企業は全て対象
for company in os.listdir(saving_path):
    crowling_folders = os.listdir(saving_path + '/' + company)
    company_name = company.split('-')[1]
    print(company_name)
    try:
        company_num = name_list.index(company_name)
        #前回スクレイピングしたコメントとフォルダの時間を確認
        pre_folder_time_tmp = newest_csv.iloc[company_num,4]
        pre_folder_time = dt.strptime(pre_folder_time_tmp,'%Y-%m-%d %H:%M:%S')
        pre_comment_time_tmp = newest_csv.iloc[company_num,1]
        pre_comment_time = dt.strptime(pre_comment_time_tmp,'%Y-%m-%d %H:%M:%S')
    except:
        #ない場合は、適当な値を取得
        pre_folder_time = dt.strptime('1930/04/01', '%Y/%m/%d') 
        pre_comment_time = dt.strptime('1930/04/01', '%Y/%m/%d') 
        
    #クローリングしたフォルダのクローリングした日付を取得してスクレイピングするべきフォルダのみ選択する
    crawling_timelist = [dt.strptime(folder,'%Y-%m-%d-%H-%M-%S') for folder in crowling_folders]
    crowling_folders = np.array(crowling_folders)
    scraping_folders = crowling_folders[np.array(crawling_timelist) > pre_folder_time]
    #前回のコメントより後のコメントで、一意なもののみ取得
    records = get_comment([saving_path + '/' + company + '/' +folder for folder in list(scraping_folders)])
    records_time = [record[2:4] for record in records]
    records_time = [dt.strptime(record_time[0]+'-'+record_time[1],'%Y/%m/%d-%H:%M') for record_time in records_time]
    records = np.array(records)
    records = records[np.array(records_time) > pre_comment_time]
    records_data_frame = pd.DataFrame(records)
    unique_scraping_records = records_data_frame.drop_duplicates()
    now = str(dt.today()).replace(' ','-').split('.')[0].replace(':','-')
    if(len(unique_scraping_records) == 0):
        write_scraping_update(company=company_name,up_time_1=pre_comment_time,up_time_2=pre_comment_time,re_time=pre_comment_time,last_folder = str(max(np.array(crawling_timelist+[pre_folder_time]))),check_result='Not update',w_a='a')
    else:
        try:#すでに記録用のcsvがあるつもりで書き込み開始
            with open(result_path + '/' + company_name + '/' + company_name + '.csv', 'a',encoding='utf-8') as f:
                unique_scraping_records.to_csv(f, header=False,index = False,encoding='utf-8')
        except:#ない場合は新しく作成
            os.mkdir(result_path + '/' + company_name)
            unique_scraping_records.to_csv(result_path + '/' + company_name + '/' + company_name + '.csv', encoding = 'utf-8',header = ['概要','企業','予想日','予想時刻','売買','ユーザ名','所持ポイント','登録時株価','取得ポイント','目標株価','予想期間','理由','コメント','データ取得時株価','データ取得日','データ取得時間','参照html'],index=False)

        unique_scraping_records.to_csv(result_path + '/' + company_name + '/' + now + '.csv', encoding = 'utf-8', index=False)
        #更新情報に書き込んで終了
    
        times = [dt.strptime(x[0] + '-' + x[1] ,'%Y/%m/%d-%H:%M') for x in np.array(unique_scraping_records[[2,3]])]
        result = max(times) > pre_comment_time and min(times) > pre_comment_time
        write_scraping_update(company=company_name,up_time_1=str(max(times)),up_time_2=str(min(times)),re_time=pre_comment_time,last_folder = str(max(np.array(crawling_timelist+[pre_folder_time]))),check_result=result,w_a='a')

    
        
    