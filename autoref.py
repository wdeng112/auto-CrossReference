import gspread
import pandas as pd
import numpy as np
from df2gspread import df2gspread as d2g
import pygsheets 
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials

scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
gc1 = pygsheets.authorize(service_account_file='/Users/xdeng/Desktop/RevealModern/client_secret.json')
sh = gc1.open_by_url('https://docs.google.com/spreadsheets/d/1vGQV2Q80c00EyWt4Sk5A4ScYPJkj6N1kY7tBbo7gYJs/edit#gid=1345053912')
casegenerated =sh[5]
internal = sh[6]

gc = gspread.authorize(credentials)


#must follow naming convention as displayed

needles = (gc.open_by_url("https://docs.google.com/spreadsheets/d/1vGQV2Q80c00EyWt4Sk5A4ScYPJkj6N1kY7tBbo7gYJs/edit#gid=1345053912")).sheet1# insert proper url for needles data worksheet
callrail = (gc.open_by_url("https://docs.google.com/spreadsheets/d/1vGQV2Q80c00EyWt4Sk5A4ScYPJkj6N1kY7tBbo7gYJs/edit#gid=442361933")).worksheet("CallRail")# insert proper url for needles data worksheet
facebook = (gc.open_by_url("https://docs.google.com/spreadsheets/d/1vGQV2Q80c00EyWt4Sk5A4ScYPJkj6N1kY7tBbo7gYJs/edit#gid=1365168323")).worksheet("Facebook Leads")# insert proper url for needles data worksheet
website = (gc.open_by_url("https://docs.google.com/spreadsheets/d/1vGQV2Q80c00EyWt4Sk5A4ScYPJkj6N1kY7tBbo7gYJs/edit#gid=600684897")).worksheet("Website")# insert proper url for needles data worksheet
testsheet = (gc.open_by_url("https://docs.google.com/spreadsheets/d/1vGQV2Q80c00EyWt4Sk5A4ScYPJkj6N1kY7tBbo7gYJs/edit#gid=1391745835")).worksheet("Test Sheet")


needlelist = needles.get_all_values() # needles index 0-24
crlist = callrail.get_all_values()# callrail index 0-39
fblist = facebook.get_all_values() #facebook leads index 0-16
wlist = website.get_all_values() # website index 0-4
testlist = testsheet.get_all_values()
columns = ["Last Name", "First Name", "Source", "Source Type", 
"Phone Number", "Email", "Case Number", "Date Converted"] # format of the final google sheets results


def removeDuplicates(listofElements):
    uniqueList = []
    for elem in listofElements:
        if elem not in uniqueList:
            uniqueList.append(elem)
    return uniqueList

customerDisplay = pd.DataFrame(columns=columns)

nt = needlelist[0]
needlelist.pop(0)
crt = crlist[0]
crlist.pop(0)
fbt = fblist[0]
fblist.pop(0)
wt = wlist[0]
wlist.pop(0)

needlelist = pd.DataFrame(needlelist, columns = nt)
crlist = pd.DataFrame(crlist, columns = crt)
fblist = pd.DataFrame(fblist, columns = fbt)
wlist = pd.DataFrame(wlist, columns = wt)

intcolumn = nt +crt +fbt +wt

internalDisplay = pd.DataFrame(columns = intcolumn)




def getValues():
    indexs=[]


    for inde in range(0,crlist.shape[0]):
        carphone = list(np.where(needlelist["car_phone"]==crlist.loc[inde, 'Phone'])[0])
        if (len(carphone) != 0):
            indexs.append(carphone[0])
            customerDisplay.at[carphone[0],'Last Name']=needlelist.loc[inde, 'last_long_name']
            customerDisplay.at[carphone[0],'First Name']=needlelist.loc[inde, 'first_name']
            customerDisplay.at[carphone[0],'Source']=crlist.loc[inde, 'Source']
            customerDisplay.at[carphone[0],'Source Type']='Phone Call'
            customerDisplay.at[carphone[0],'Phone Number']=needlelist.loc[inde, 'car_phone']
            customerDisplay.at[carphone[0],'Email']=needlelist.loc[inde, 'email']
            customerDisplay.at[carphone[0],'Case Number']=needlelist.loc[inde, 'casenum']
            customerDisplay.at[carphone[0],'Date Converted']=needlelist.loc[inde, 'date_opened']
            # internallist = pd.concat([needlelist[inde],crlist[inde]],axis=0, ignore_index=False)
            
            # internalDisplay.append(internallist)
        inde+=1
    for kk in range(0,fblist.shape[0]):
        fbphone = list(np.where(needlelist["car_phone"]==fblist.loc[kk, 'phone_number'])[0])
        if (len(fbphone) != 0):
            indexs.append(fbphone[0])
            customerDisplay.at[fbphone[0],'Last Name']=needlelist.loc[inde, 'last_long_name']
            customerDisplay.at[fbphone[0],'First Name']=needlelist.loc[inde, 'first_name']
            customerDisplay.at[fbphone[0],'Source']=fblist.loc[inde, 'platform']
            customerDisplay.at[fbphone[0],'Source Type']='Facebook Forms'
            customerDisplay.at[fbphone[0],'Phone Number']=needlelist.loc[inde, 'car_phone']
            customerDisplay.at[fbphone[0],'Email']=needlelist.loc[inde, 'email']
            customerDisplay.at[fbphone[0],'Case Number']=needlelist.loc[inde, 'casenum']
            customerDisplay.at[fbphone[0],'Date Converted']=needlelist.loc[inde, 'date_opened']
        kk+=1

    for gg in range(0,wlist.shape[0]):
        webphone = list(np.where(needlelist["car_phone"]==wlist.loc[gg, 'Phone'])[0])
        if (len(webphone) != 0):
            indexs.append(webphone[0])
            customerDisplay.at[webphone[0],'Last Name']=needlelist.loc[inde, 'last_long_name']
            customerDisplay.at[webphone[0],'First Name']=needlelist.loc[inde, 'first_name']
            customerDisplay.at[webphone[0],'Source']='Website'
            customerDisplay.at[webphone[0],'Source Type']='Forms'
            customerDisplay.at[webphone[0],'Phone Number']=needlelist.loc[inde, 'car_phone']
            customerDisplay.at[webphone[0],'Email']=needlelist.loc[inde, 'email']
            customerDisplay.at[webphone[0],'Case Number']=needlelist.loc[inde, 'casenum']
            customerDisplay.at[webphone[0],'Date Converted']=needlelist.loc[inde, 'date_opened']
        gg+=1

    indexs = removeDuplicates(indexs)
   
def updateSheet():
    if (len(casegenerated.get_all_values()) >= 1):
        getValues()
        customerDisplay.replace('','Not Provided', inplace=True)
        casegenerated.set_dataframe(customerDisplay,(1,1))
    else:
        caselist = casegenerated.get_as_df()
        getValues()
        customerDisplay.replace('','Not Provided', inplace=True)
        customerDisplay.merge(caselist)
        customerDisplay.drop_duplicates(keep=False)
        casegenerated.set_dataframe(customerDisplay,(1,1))


updateSheet()


# getValues()
# customerDisplay.replace('','Not Provided', inplace=True)
# print (customerDisplay)
# casegenerated.set_dataframe(customerDisplay,(1,1))

# print(customerDisplay.iterrows)









    



