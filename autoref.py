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
sh = gc1.open_by_url('https://docs.google.com/spreadsheets/d/1Tfl-nV2L3D6wZTMmsntteb51MjG7WbpSzaWOJPI4uWI/edit#gid=1924399580')
casegenerated =sh[0]
internal = sh[1]

gc = gspread.authorize(credentials)


#must follow naming convention as displayed

needles = (gc.open_by_url("https://docs.google.com/spreadsheets/d/1Tfl-nV2L3D6wZTMmsntteb51MjG7WbpSzaWOJPI4uWI/edit#gid=240631845")).worksheet("Ben Abbott Needles Data")
callrail = (gc.open_by_url("https://docs.google.com/spreadsheets/d/1Tfl-nV2L3D6wZTMmsntteb51MjG7WbpSzaWOJPI4uWI/edit#gid=1471086713")).worksheet("Callrail")
facebook = (gc.open_by_url("https://docs.google.com/spreadsheets/d/1Tfl-nV2L3D6wZTMmsntteb51MjG7WbpSzaWOJPI4uWI/edit#gid=1640356431")).worksheet("Facebook Lead Ads")
website = (gc.open_by_url("https://docs.google.com/spreadsheets/d/1Tfl-nV2L3D6wZTMmsntteb51MjG7WbpSzaWOJPI4uWI/edit#gid=591557577")).worksheet("Website Forms")
crform = (gc.open_by_url("https://docs.google.com/spreadsheets/d/1Tfl-nV2L3D6wZTMmsntteb51MjG7WbpSzaWOJPI4uWI/edit#gid=720861243")).worksheet("CallRail Forms")


needlelist = needles.get_all_values() 
crlist = callrail.get_all_values()
fblist = facebook.get_all_values() 
wlist = website.get_all_values() 
crflist = crform.get_all_values()
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
crf = crflist[0]
crflist.pop(0)

needlelist = pd.DataFrame(needlelist, columns = nt)
crlist = pd.DataFrame(crlist, columns = crt)
fblist = pd.DataFrame(fblist, columns = fbt)
wlist = pd.DataFrame(wlist, columns = wt)
crflist = pd.DataFrame(crflist, columns =crf)

intcolumn = nt +crt +fbt +wt +crf

internalDisplay = pd.DataFrame(columns = intcolumn)


# print (crflist.columns.values)

def getValues():
    indexs=[]


    for inde in range(0,crlist.shape[0]):
        carphone = list(np.where(needlelist["car_phone"]==crlist.loc[inde, 'Phone Number'])[0])
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
            customerDisplay.at[fbphone[0],'Source Type']=fblist.loc[inde, 'type']
            customerDisplay.at[fbphone[0],'Phone Number']=needlelist.loc[inde, 'car_phone']
            customerDisplay.at[fbphone[0],'Email']=needlelist.loc[inde, 'email']
            customerDisplay.at[fbphone[0],'Case Number']=needlelist.loc[inde, 'casenum']
            customerDisplay.at[fbphone[0],'Date Converted']=needlelist.loc[inde, 'date_opened']
        kk+=1

    # for gg in range(0,wlist.shape[0]):
    #     webphone = list(np.where(needlelist["car_phone"]==wlist.loc[gg, 'Phone'])[0])
    #     if (len(webphone) != 0):
    #         indexs.append(webphone[0])
    #         customerDisplay.at[webphone[0],'Last Name']=needlelist.loc[inde, 'last_long_name']
    #         customerDisplay.at[webphone[0],'First Name']=needlelist.loc[inde, 'first_name']
    #         customerDisplay.at[webphone[0],'Source']='Website'
    #         customerDisplay.at[webphone[0],'Source Type']='Forms'
    #         customerDisplay.at[webphone[0],'Phone Number']=needlelist.loc[inde, 'car_phone']
    #         customerDisplay.at[webphone[0],'Email']=needlelist.loc[inde, 'email']
    #         customerDisplay.at[webphone[0],'Case Number']=needlelist.loc[inde, 'casenum']
    #         customerDisplay.at[webphone[0],'Date Converted']=needlelist.loc[inde, 'date_opened']
    #     gg+=1
    for ju in range(0,crflist.shape[0]):
        formphone = list(np.where(needlelist["car_phone"]==crflist.loc[ju,'Phone'])[0])
        if (len(formphone) != 0):
            indexs.append(formphone[0])
            customerDisplay.at[formphone[0],'Last Name']=needlelist.loc[inde, 'last_long_name']
            customerDisplay.at[formphone[0],'First Name']=needlelist.loc[inde, 'first_name']
            customerDisplay.at[formphone[0],'Source']= 'Facebook'
            customerDisplay.at[formphone[0],'Source Type']='Forms'
            customerDisplay.at[formphone[0],'Phone Number']=needlelist.loc[inde, 'car_phone']
            customerDisplay.at[formphone[0],'Email']=needlelist.loc[inde, 'email']
            customerDisplay.at[formphone[0],'Case Number']=needlelist.loc[inde, 'casenum']
            customerDisplay.at[formphone[0],'Date Converted']=needlelist.loc[inde, 'date_opened']
        ju+=1


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









    



