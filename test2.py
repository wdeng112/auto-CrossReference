import gspread
import pandas as pd
import numpy as np
import pygsheets 
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials

scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
gc1 = pygsheets.authorize(service_account_file = r'C:\Users\Reveal Modern 1\Desktop\test\client_secret.json')
sh = gc1.open_by_url('https://docs.google.com/spreadsheets/d/1Tfl-nV2L3D6wZTMmsntteb51MjG7WbpSzaWOJPI4uWI/edit?ts=5c64a129#gid=1924399580')
casegenerated =sh[0]

gc = gspread.authorize(credentials)

#must follow naming convention as displayed

needles = (gc.open_by_url("https://docs.google.com/spreadsheets/d/1Tfl-nV2L3D6wZTMmsntteb51MjG7WbpSzaWOJPI4uWI/edit?ts=5c64a129#gid=240631845")).worksheet("Ben Abbott Needles Data")# insert proper url for needles data worksheet
callrail = (gc.open_by_url("https://docs.google.com/spreadsheets/d/1Tfl-nV2L3D6wZTMmsntteb51MjG7WbpSzaWOJPI4uWI/edit?ts=5c64a129#gid=1471086713")).worksheet("CallRail")# insert proper url for needles data worksheet
facebook = (gc.open_by_url("https://docs.google.com/spreadsheets/d/1Tfl-nV2L3D6wZTMmsntteb51MjG7WbpSzaWOJPI4uWI/edit?ts=5c64a129#gid=1640356431")).worksheet("Facebook Lead Ads")# insert proper url for needles data worksheet
website = (gc.open_by_url("https://docs.google.com/spreadsheets/d/1Tfl-nV2L3D6wZTMmsntteb51MjG7WbpSzaWOJPI4uWI/edit?ts=5c64a129#gid=591557577")).worksheet("Website Forms")# insert proper url for needles data worksheet
callrailform = (gc.open_by_url("https://docs.google.com/spreadsheets/d/1Tfl-nV2L3D6wZTMmsntteb51MjG7WbpSzaWOJPI4uWI/edit?ts=5c64a129#gid=720861243")).worksheet("CallRail Forms")
internalreport = (gc.open_by_url("https://docs.google.com/spreadsheets/d/1Tfl-nV2L3D6wZTMmsntteb51MjG7WbpSzaWOJPI4uWI/edit?ts=5c64a129#gid=664120587")).worksheet("All Data")


needlelist = needles.get_all_values()
crlist = callrail.get_all_values()
fblist = facebook.get_all_values() 
wlist = website.get_all_values()



columns = ["Last Name", "First Name", "Source", "Source Type", 
"Phone Number", "Email", "Case Number", "Date Converted"]

finalFrame = pd.DataFrame(columns=columns)


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




def remove_duplicates(l):
    return list(set(l))

def getValues():
    indexs=[]


    for inde in range(0,crlist.shape[0]):
        carphone = list(np.where(needlelist["car_phone"]==crlist.loc[inde, 'Phone'])[0])
        if (len(carphone) != 0):
            indexs.append(carphone[0])
            finalFrame.at[carphone[0],'Last Name']=needlelist.loc[inde, 'last_long_name']
            finalFrame.at[carphone[0],'First Name']=needlelist.loc[inde, 'first_name']
            finalFrame.at[carphone[0],'Source']=crlist.loc[inde, 'Source']
            finalFrame.at[carphone[0],'Source Type']='Phone Call'
            finalFrame.at[carphone[0],'Phone Number']=needlelist.loc[inde, 'car_phone']
            finalFrame.at[carphone[0],'Email']=needlelist.loc[inde, 'email']
            finalFrame.at[carphone[0],'Case Number']=needlelist.loc[inde, 'casenum']
            finalFrame.at[carphone[0],'Date Converted']=needlelist.loc[inde, 'date_opened']
        inde+=1

    for kk in range(0,fblist.shape[0]):
        fbphone = list(np.where(needlelist["car_phone"]==fblist.loc[kk, 'phone_number'])[0])
        if (len(fbphone) != 0):
            indexs.append(fbphone[0])
            finalFrame.at[fbphone[0],'Last Name']=needlelist.loc[inde, 'last_long_name']
            finalFrame.at[fbphone[0],'First Name']=needlelist.loc[inde, 'first_name']
            finalFrame.at[fbphone[0],'Source']=fblist.loc[inde, 'platform']
            finalFrame.at[fbphone[0],'Source Type']='Facebook Forms'
            finalFrame.at[fbphone[0],'Phone Number']=needlelist.loc[inde, 'car_phone']
            finalFrame.at[fbphone[0],'Email']=needlelist.loc[inde, 'email']
            finalFrame.at[fbphone[0],'Case Number']=needlelist.loc[inde, 'casenum']
            finalFrame.at[fbphone[0],'Date Converted']=needlelist.loc[inde, 'date_opened']
        kk+=1

    for gg in range(0,wlist.shape[0]):
        webphone = list(np.where(needlelist["car_phone"]==wlist.loc[gg, 'Phone'])[0])
        if (len(webphone) != 0):
            indexs.append(webphone[0])
            finalFrame.at[webphone[0],'Last Name']=needlelist.loc[inde, 'last_long_name']
            finalFrame.at[webphone[0],'First Name']=needlelist.loc[inde, 'first_name']
            finalFrame.at[webphone[0],'Source']='Website'
            finalFrame.at[webphone[0],'Source Type']='Forms'
            finalFrame.at[webphone[0],'Phone Number']=needlelist.loc[inde, 'car_phone']
            finalFrame.at[webphone[0],'Email']=needlelist.loc[inde, 'email']
            finalFrame.at[webphone[0],'Case Number']=needlelist.loc[inde, 'casenum']
            finalFrame.at[webphone[0],'Date Converted']=needlelist.loc[inde, 'date_opened']
        gg+=1

    indexs = remove_duplicates(indexs)
   
def updateSheet():
    if (len(casegenerated.get_all_values()) >= 1):
        getValues()
        finalFrame.replace('','Not Provided', inplace=True)
        casegenerated.set_dataframe(finalFrame,(1,1))
    else:
        caselist = casegenerated.get_as_df()
        getValues()
        finalFrame.replace('','Not Provided', inplace=True)
        finalFrame.merge(caselist)
        finalFrame.drop_duplicates(keep=False)
        casegenerated.set_dataframe(finalFrame,(1,1))


updateSheet()














    



