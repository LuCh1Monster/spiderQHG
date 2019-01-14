import os
import shutil
import smtplib
import time
import warnings
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import numpy as np
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from sqlalchemy import create_engine

from .config import loginAccount, loginPassword

warnings.filterwarnings('ignore')

options = Options()
# options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('disable-infobars')
options.add_experimental_option("excludeSwitches", ["ignore-certificate-errors"])
# options.add_argument('--disable-dev-shm-usage')

html = webdriver.Chrome(executable_path="d:\\anaconda3\\chromedriver.exe", chrome_options=options)
html.implicitly_wait(60)
html.maximize_window()

html.get('http://back.quhgo.com/index')
time.sleep(0.5)
html.find_element_by_css_selector('#userPhoneNumber').send_keys(loginAccount) # 账号
time.sleep(0.5)

html.find_element_by_css_selector('#loginMessageCode').send_keys(loginPassword) # 验证码2
time.sleep(1)
# 登录
html.find_element_by_css_selector('#loginMessageCode').send_keys(Keys.ENTER)
time.sleep(1)
html.find_element_by_xpath('//*[@id="menu"]/li/a').click()
time.sleep(1)
# 点击进入 S2预期用户列表
html.find_element_by_xpath('//*[@id="menu"]/li/ul/li/a').click()
time.sleep(1)
html.switch_to_frame('external-frame')
html.find_element_by_css_selector('#export').click()
time.sleep(180)

# move download file to `query_list` directory
day = datetime.now().strftime('%Y%m%d')
files = [file for file in os.listdir('./data/query_list/') if file.startswith(day) and file.endswith('.xls')]

if len(files) > 0:
    shutil.rmtree('./data/query_list/')
    os.mkdir('./data/query_list/')
    download_dir = 'C:\\Users\\yumingmin\\Downloads\\'
    down_file = [download_dir + fn for fn in  os.listdir(download_dir) if fn.endswith('.xls')][0]
    shutil.move(down_file, './data/query_list/')
else:
    download_dir = 'C:\\Users\\yumingmin\\Downloads\\'
    down_file = [download_dir + fn for fn in  os.listdir(download_dir) if fn.endswith('.xls')][0]
    shutil.move(down_file, './data/query_list/')

# 
download_file = ['./data/query_list/' + file for file in os.listdir('./data/query_list/') if file.endswith('.xls')][0]
df_list = pd.read_excel(download_file)
df_list_31 = df_list[df_list['逾期天数'] == 31]

now_fmt = datetime.now().strftime("%Y-%m-%d")
if os.path.exists('./data/crawled_data/data_%s' % now_fmt):
    pass
else:
    os.mkdir('./data/crawled_data/data_%s' % now_fmt)


tbs_baseInfo, tbs_userContacts = [], []
error_orderid_lst = []

for orderid in df_list_31['订单号'].unique():
    html.refresh()
    html.find_element_by_xpath('//*[@id="menu"]/li/a').click()
    time.sleep(1)
    html.find_element_by_xpath('//*[@id="menu"]/li/ul/li/a').click()
    time.sleep(0.5)
    html.switch_to_frame('external-frame')
    
    html.find_element_by_css_selector('#orderId').send_keys(str(orderid)) # 订单号 341520
    time.sleep(0.5)
    # 查询
    html.find_element_by_xpath('//*[@id="formpro"]/div/div[5]/button').click()
    time.sleep(1)

    info1 = pd.read_html(html.page_source)[0]
    info1 = info1[info1.columns[:-1]]
    
    if len(info1) > 0:
        html.find_element_by_xpath('//*[@id="bigDataList"]/tbody/tr/td[14]/a[1]').click()
        time.sleep(0.5)
        info_details_lst = pd.read_html(html.page_source)
        info_details_lst[0].columns = ['c_1', 'c_2', 'c_3', 'c_4']
        info_dct = dict(zip(info_details_lst[0].c_1, info_details_lst[0].c_2))
        info_dct.update(dict(zip(info_details_lst[0].c_3, info_details_lst[0].c_4)))
        info_dct.pop(np.nan)
        keep_cols = ['用户ID', '身份证号码', '学历', '婚姻状态', '现居地址', '居住时长', '银行卡', '银行卡所属银行','是否黑名单']
        info2 = pd.DataFrame(info_dct, index=range(1))[keep_cols]
        df_baseInfo = pd.concat([info1,info2], axis=1)
        
        df_userContacts = pd.read_html(html.page_source, attrs={"class": "table table-hover"})[0]
        df_userContacts['姓名'] = df_baseInfo['姓名'].values[0]
        df_userContacts['用户手机号'] = df_baseInfo['手机号'].values[0]
        df_userContacts['身份证号码'] = df_baseInfo['身份证号码'].values[0]
        keep_cols2 = ['姓名', '用户手机号', '身份证号码', '备注名', '手机号']
        df_userContacts = df_userContacts[keep_cols2]
        tbs_baseInfo.append(df_baseInfo)
        tbs_userContacts.append(df_userContacts)
        html.refresh()
        time.sleep(3)
    else:
        error_orderid_lst.append(orderid)
        pass
    
html.close()

df_baseInfo_all = pd.concat(tbs_baseInfo)
df_userContacts_all = pd.concat(tbs_userContacts)
df_userContacts_all['手机号'] = df_userContacts_all['手机号'].astype(str)
df_userContacts_all['手机号'] = df_userContacts_all['手机号'].map(lambda s: s[:-2] if s.endswith('.0') else s)
df_userContacts_all.rename(columns={'备注名': '联系人姓名', '手机号': '联系人号码'}, inplace=True)

print(len(error_orderid_lst))
print(len(df_list_31))
print(len(df_baseInfo_all))
print(len(df_userContacts_all['身份证号码'].unique()))

outputfile_csv = './data/crawled_data/data_%s/user_baseinfo.csv' % now_fmt
outputfile_xlsx = './data/crawled_data/data_%s/user_baseinfo.xlsx' % now_fmt
df_baseInfo_all.to_csv(outputfile_csv, index=False, encoding='utf8')
df_baseInfo_all.to_excel(outputfile_xlsx, index=False, encoding='utf8')

outputfile2_csv = './data/crawled_data/data_%s/user_contacts.csv' % now_fmt
outputfile2_xlsx = './data/crawled_data/data_%s/user_contacts.xlsx' % now_fmt
df_userContacts_all.to_csv(outputfile2_csv, index=False, encoding='utf8')
df_userContacts_all.to_excel(outputfile2_xlsx, index=False, encoding='utf8')

userInfo_rowsnum = df_baseInfo_all.shape[0]

def send_email(filename, dt, rowsnum):
    
    fn = filename
    run_date = dt
    rows_num =rowsnum
    
    server = smtplib.SMTP()
    server.connect('smtp.partner.outlook.cn')
    server.starttls()
    server.login(user='yumingmin@ppdai.com',
                 password='PPDcs326029')
    
    context = {'run_date': run_date, 'rows_num': rows_num}
    
    msgText = """<html>
    <body>
        <br><br>
        DATE: {run_date}, <br>
        新案总数: {rows_num}<br>
    </body></html>""".format(**context)
    msgtext = MIMEText(msgText, 'html', 'utf-8')

    msg = MIMEMultipart('related')
    msg.attach(msgtext)

    recipients = ['yumingmin@ppdai.com', 'mingjiao02@ppdai.com', 'chenwei07@ppdai.com', 
                  'zhangzhongzheng@ppdai.com', 'cs_jigou@ppdai.com']
    msg['from'] = 'yumingmin@ppdai.com'
    msg['to'] = ','.join(recipients)
    msg['subject'] = '趣回购爬虫{run_date}当日新案信息'.format(**context)

    att = MIMEText(open('%s'% fn, 'rb').read(), 'base64', 'utf-8')
    att["Content-Type"] = 'application/octet-stream'
    att["Content-Disposition"] = 'attachment; filename="%s"'% fn.split("/")[-1]

    msg.attach(att)
    server.sendmail(msg['from'], 
                    msg['to'].split(','), 
                    msg.as_string()) 
    server.quit()

# 发送邮件
send_email(filename=outputfile_xlsx, dt=now_fmt, rowsnum=userInfo_rowsnum)
# 导入 mysql
now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
df_baseInfo_all['record_creation_time'] = now
df_baseInfo_all['record_update_time'] = now
df_baseInfo_all['dt'] = now_fmt
df_userContacts_all['record_creation_time'] = now
df_userContacts_all['record_update_time'] = now
df_userContacts_all['dt'] = now_fmt

rename_cols1 = {'订单号': 'order_id', 
                '姓名': 'user_name', 
                '手机号': 'user_mob_phone', 
                '借款金额': 'borrow_amount', 
                '违约金': 'user_penalty', 
                '滞纳金': 'user_latefees', 
                '累计减免金额': 'accum_reduction_amount', 
                '累计应还金额': 'accum_repayed_amount', 
                '已还金额': 'repayed_amount',
                '放款时间': 'lending_time', 
                '应还款时间': 'should_repayment_time', 
                '逾期天数': 'overdue_days', 
                '渠道': 'channel', 
                '分案时间': 'allot_time', 
                '婚姻状态': 'user_mar', 
                '学历': 'user_edu', 
                '居住时长': 'residence_duration', 
                '是否黑名单': 'is_blacklist',
                '注册手机号': 'user_register_mobphone', 
                '现居地址': 'user_residence_addr', 
                '用户ID': 'user_id', 
                '身份证号码': 'user_idcard', 
                '银行卡': 'user_bind_bank_card', 
                '银行卡所属银行': 'user_bind_bank'}

df_baseInfo_all_reanme = df_baseInfo_all.rename(columns=rename_cols1)

rename_cols2 = { '姓名': 'user_name', 
               '用户手机号': 'user_mob_phone', 
               '身份证号码': 'user_idcard', 
               '联系人姓名': 'user_contacts_name',
               '联系人号码': 'user_contacts_mobphone'}
df_userContacts_all_rename = df_userContacts_all.rename(columns=rename_cols2)

engine = create_engine('mysql+pymysql://cuishou:PPDcs2018@10.128.108.42:3306/bpo?charset=utf8',
                                          encoding='utf8', convert_unicode=True)
df_baseInfo_all_reanme.to_sql('quhuigou_user_baseinfo', engine, if_exists='append', index=False, chunksize=10000)
df_userContacts_all_rename.to_sql('quhuigou_user_contacts', engine, if_exists='append', index=False, chunksize=10000)

rm_dir = './data/query_list'
os.chdir(rm_dir)
exists_files = [os.remove(file) for file in os.listdir('.') if file.endswith('.xls')]
print('task finished!')
