from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import schedule
import time
from bs4 import BeautifulSoup
import time
from datetime import datetime
import csv
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy
#  Create a class


df_previous = pd.DataFrame()

df_final = pd.DataFrame()

def filter_list(data2):
    data1 =[]
    position = 0
    length1 = len(data2)
    x = length1 - 1
    data2.pop(x)
    length = len(data2)

    data = [ele for ele in data2 if ele != []]
    print('Data beolow without blanks')

    print('deleted extra rows')
    length = len(data)

    for i in range(length):
        flag = 0
        string = data[i][8]
        for j in string:
            if (j == '('):
                flag = 1
                break
        if (flag == 0):
            data1.append(data[i])

    return  data1



def type_conversion(df_final):
    df_final.QTY = df_final.QTY.astype(int)
    df_final.Avg = df_final.Avg.str.replace(",", "").astype(float)
    df_final.Avg_price = df_final.Avg_price.str.replace(",", "").astype(float)
    df_final.position = df_final.position.str.replace(",", "").astype(float)
    df_final.positionNET = df_final.positionNET.str.replace(",", "").astype(float)
    df_final.Avi = df_final.Avi.str.replace(",", "").astype(float)
    df_final.Net = df_final.Net.str.replace(",", "").astype(float)
    df_final.Avg_qty = df_final.Avg_qty.str.replace(",", "").astype(float)




def compare(df_current,df_previous):
    df1 = df_previous
    df2 = df_current

    df1.columns = ['Name', 'Subname', 'Subname2', 'QTY', 'Avg', 'Avg_price', 'position', 'positionNET',
                       'Avi',
                       'Avg_qty', 'Net']

    df2.columns = ['Name', 'Subname', 'Subname2', 'QTY', 'Avg', 'Avg_price', 'position', 'positionNET',
                   'Avi',
                   'Avg_qty', 'Net']

    df_join = df1.merge(right=df2,
                        left_on=df1.columns.to_list(),
                        right_on=df2.columns.to_list(),
                        how='outer')
    # %%
    df1.rename(columns=lambda x: x + '_file1', inplace=True)
    df2.rename(columns=lambda x: x + '_file2', inplace=True)
    # %%
    df_join = df1.merge(right=df2,
                        left_on=df1.columns.to_list(),
                        right_on=df2.columns.to_list(),
                        how='outer')

    records_present_in_df2_not_in_df1 = df_join.loc[
        df_join[df1.columns.to_list()].isnull().all(axis=1), df2.columns.to_list()]


    # %%
    df2 = records_present_in_df2_not_in_df1

    df2.columns = ['Name', 'Subname', 'Subname2', 'QTY', 'Avg', 'Avg_price', 'position', 'positionNET',
                   'Avi',
                   'Avg_qty', 'Net']

    # print(df2)

    return df2



def compare_col_wise(df_current,df_previous):
    df2 = df_previous
    df1 = df_current

    df1.columns = ['Name', 'Subname', 'Subname2', 'QTY', 'Avg', 'Avg_price', 'position', 'positionNET',
                   'Avi',
                   'Avg_qty', 'Net']

    df2.columns = ['Name', 'Subname', 'Subname2', 'QTY', 'Avg', 'Avg_price', 'position', 'positionNET',
                   'Avi',
                   'Avg_qty', 'Net']



    change_avg = df2[~df2.positionNET.isin(df1.positionNET)]
    # print('Printing change in Avg here')
    # print(change_avg)
    df3 = change_avg
    # print(df3)

    return df3


def compare_main(current,previous):
    if(len(current.index) == len(previous.index)):
        print('Enter the first if statement of the compare_main function')
        print('value of current if the rows len is same ')
        print(current)
        print('value of previous in same row len is')
        print(previous)
        y =compare_col_wise(current,previous)
        print('The value of y is printed here')
        print(y)
        return y
        print('Returned Y to df_final')
        # Return Data to List Final
    else :
        print('Entered the else part of the compare_main function')
        temp_df = compare(current,previous)
        print(temp_df)
        previous.append(temp_df, ignore_index=True)
        print('The value of x is printed here')
        x= compare_col_wise(current,previous)
        print('The value returned from the compare function is :')
        print(x)
        return x

#
#       #Temp panda dataframe

def driver(df_previous):
    try :
        i = 1
        print(i)
        now = datetime.now()

        # 12-hour format
        print(now.strftime('%Y/%m/%d %I:%M:%S'))

        chrome_browser = webdriver.Chrome('# Your ChromeDriver Path')
        chrome_browser.maximize_window()
        chrome_browser.get('https://shivam.tradeoval.com/dbex/')

        member_id = chrome_browser.find_element_by_id('txtMemberCode')
        member_id.clear()
        member_id.send_keys('Username')

        member_username = chrome_browser.find_element_by_id('txtUsername')
        member_username.clear()
        member_username.send_keys('MemberCode')

        member_password = chrome_browser.find_element_by_id('txtPassword ')
        member_password.clear()
        member_password.send_keys('Password')

        login_button = chrome_browser.find_element_by_tag_name('button')
        login_button.click()

        try:
            element = WebDriverWait(chrome_browser, 10).until(
                EC.presence_of_element_located((By.LINK_TEXT, "Position Report"))
            )
        except:
            chrome_browser.quit()

        element.click()

        try:
            get_position = WebDriverWait(chrome_browser, 10).until(
                EC.presence_of_element_located((By.XPATH, "//*[@id='scopeDiv']/div[1]/form/div/div[9]/div/button"))
            )
        except:
            chrome_browser.quit()

        get_position.click()

        time.sleep(15)

        print('Reached before finding table')

        soup = BeautifulSoup(chrome_browser.page_source, "html.parser")

        table = soup.find('table', class_='table table-striped table-bordered nowrap lh0 ng-scope dataTable')
        table_rows = table.findAll('tr')



        data = []

        for tr in table_rows:
            td = tr.find_all('td')
            row = [i.text for i in td]
            data.append(row)

        data = filter_list(data)

        df_csv_in = pd.DataFrame(data)
        df_csv_in.to_csv('scrapped_data_latest1.csv', encoding='utf-8',header=False, index=False)
        print('The data transfered to csv')

        engine = create_engine('mssql+pymssql://user:passworda@IP-address/Table_name')

        df_current = pd.DataFrame(data)
        df_current.columns = ['Decoy', 'Name', 'Subname', 'Subname2', 'QTY', 'Avg', 'Avg_price', 'position', 'positionNET',
                              'Avi',
                              'Avg_qty', 'Net', 'Nets']
        y = df_current.drop('Decoy', axis=1, inplace=True)
        z = df_current.drop('Nets', axis=1, inplace=True)
        a = df_current.drop([0], axis=0, inplace=True)

        if (df_previous.empty == True):
            df_final = df_current
            type_conversion(df_final)
        else:
            type_conversion(df_current)
            df_final = compare_main(df_current,df_previous)

        if (df_final.empty == False):
            connection = engine.connect()
            truncate_query = sqlalchemy.text("TRUNCATE TABLE table_name")
            connection.execution_options(autocommit=True).execute(truncate_query)
            print('db is truncatedd')


            df_final.to_sql(
                name='tblTestData',  # database table name
                con=engine,
                if_exists='append',
                index=False
            )

        # print('Printing value of df_final after insertion in db')
        # print(df_final)

        if(df_final.empty == True):
            df_final = pd.DataFrame()


        print(df_final)
        df_current = pd.DataFrame()
        df_previous = df_final
        df_final = pd.DataFrame()

        # print(df_data2.tolist())

        chrome_browser.quit()
        driver(df_previous)

    except Exception as e:
        print(e)
        driver(df_previous)
        chrome_browser.quit()

driver(df_previous)

