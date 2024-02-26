import configparser
from snowflake.snowpark import Session
import streamlit as st
import pandas as pd
import json

def getSession():
    config = configparser.ConfigParser()
    config.read("secrets.toml")
    session = Session.builder.configs(dict(st.secrets.snow)).create() 
    return session

def getSavedScenario():
    df=getSession().sql(f'''
        select * from SIZING_APP_DB.SIZING_APP_SC.SIZING_APP;
    ''').collect()
    return df

# @st.cache_data(ttl=1,show_spinner=False)
def getSavedScenarioByPartner(partner):
    df=getSession().sql(f'''
       select * from SIZING_APP_DB.SIZING_APP_SC.SIZING_APP where INPUTS:name='{partner}';
    ''').collect()
    return df

def save(id,partner,prospect,ae,size,date,template):
    df=getSession().sql(f'''
        INSERT INTO SIZING_APP_DB.SIZING_APP_SC.SIZING_APP (ID, PARTNER,PROSPECT,AE,DEAL_SIZE,DEAL_DATE, TEMPLATE) select '{id}', '{partner}','{prospect}','{ae}',{size},TO_DATE('{str(date)}') , PARSE_JSON('{json.dumps(template)}') ;
    ''').collect()
    return df

def getRoles(partner):
    df=getSession().sql(f'''
        select * from SIZING_APP_DB.SIZING_APP_SC.ROLES where connection_string ilike '%{partner}%';
    ''').collect()
    return df



def _getYOYCredits():
    df=getSession().sql(f'''
        select PARTNER_NAME as PNAME, YOYPCT as TOTCRED  from SIZING_APP_DB.SIZING_APP_SC.YOY_CREDITS;
    ''').collect()
    return df

def _getLastMonthCredits():
    df=getSession().sql(f'''
        select PARTNER_NAME as PNAME, L28DAYS as TOTCRED  from SIZING_APP_DB.SIZING_APP_SC.LAST_MONTH_CREDITS;
    ''').collect()
    return df

@st.cache_data(ttl=500,show_spinner=False)
def _getGTM():
    df=getSession().sql(f'''
        select PARTNER_NAME as PNAME, CONNECTED_CUSTOMERS, REFERENCE_ARCH ,IFF( REFERENCE_ARCH='Yes',1,0 ) as REF_ARCH, SALES_COLLATERAL, IFF( SALES_COLLATERAL='Yes',1,0 ) as SALES_COL,
        DEAL_REGS, CLOSED_REGS from SIZING_APP_DB.SIZING_APP_SC.GTM;
    ''').collect()
    return df

@st.cache_data(ttl=500,show_spinner=False)
def _getFeatures():
    df=getSession().sql(f'''
        select PARTNER_NAME as PNAME, FEATURE_NAME as FNAME from SIZING_APP_DB.SIZING_APP_SC.FEATURES;
    ''').collect()
    return df

def getPartnersNamesGTM():
    df=_getGTM()
    return pd.DataFrame(df)[['PNAME']]

def getYOYCredits(partnerName,base='real'):
   raw= pd.DataFrame(_getYOYCredits())
   pn=(raw.loc[raw['PNAME']==partnerName])['TOTCRED'].iloc[0]
   max=raw['TOTCRED'].max()
   maxP=raw.loc[raw['TOTCRED']==max]['PNAME'].iloc[0]
   if base=='cent':
    return {'conso':(pn/max)*100,"ref":100,"refPartner":maxP}
   else:
    return {'conso':pn,"ref":max,"refPartner":maxP}

def getLastMonthCredits(partnerName,base='real'):
   raw= pd.DataFrame(_getLastMonthCredits())
   pn=(raw.loc[raw['PNAME']==partnerName])['TOTCRED'].iloc[0]
   max=raw['TOTCRED'].max()
   maxP=raw.loc[raw['TOTCRED']==max]['PNAME'].iloc[0]
   if base=='cent':
    return {'conso':(pn/max)*100,"ref":100,"refPartner":maxP}
   else:
    return {'conso':pn,"ref":max,"refPartner":maxP} 

def getGTM(partnerName,kpi,base='real'):
   raw= pd.DataFrame(_getGTM())
   # TO CORRECT TYPOS... NEEDS TO BE FIXED ON DATA LAYER
   # onlyPartner=raw.loc[raw['PNAME'].str.contains(partnerName[0:3])]  
   onlyPartner=raw.loc[raw['PNAME']==partnerName]
   pn=onlyPartner[kpi].iloc[0]
   max=raw[kpi].max()
   maxP=raw.loc[raw[kpi]==max]['PNAME'].iloc[0]
   if base=='cent':
    return {'conso':(pn/max)*100,"ref":100,"refPartner":maxP}
   else:
    return {'conso':pn,"ref":max,"refPartner":maxP}       

def getFeatures(partnerName,featname):
    raw= pd.DataFrame(_getFeatures())
    onlyPartner=raw.loc[raw['PNAME']==partnerName]
    res=onlyPartner[onlyPartner['FNAME']==featname]
    return  {'conso':len(res),"ref":1,"refPartner":''}


def getAllKPI(partner):
    all=[]
    yoy=getYOYCredits(partner)
    lastmonth=getLastMonthCredits(partner)
    connected=getGTM(partner,'CONNECTED_CUSTOMERS')
    refarch=getGTM(partner,'REF_ARCH')
    salescol=getGTM(partner,'SALES_COL')
    dealreg=getGTM(partner,'DEAL_REGS')
    closereg=getGTM(partner,'CLOSED_REGS')

    snowpipe=getFeatures(partner,'Snowpipe')
    topk=getFeatures(partner,'TopK')
    qas=getFeatures(partner,'QAS')
    so=getFeatures(partner,'Search Optimization')

    all.append(yoy)
    all.append(lastmonth)
    all.append(connected)
    all.append(refarch)
    all.append(salescol)
    all.append(dealreg)
    all.append(closereg)
    all.append(snowpipe)
    all.append(topk)
    all.append(qas)
    all.append(so)


    cat=['Credits YOY','Credits Last 28 Days','Connected Customers','Reference Architecture','Sales Collateral','Deal Registrations','Close Deals','Snowpipe','TopK','QAS','Search Optimization']
    catMaxName=cat.copy()
    for idx, c in enumerate(catMaxName):
        catMaxName[idx]=formatAxis(c,all[idx])

    kpis=[]
    ref=[]
    refp=[]
    for kpi in all:
        kpis.append(float(kpi['conso']))
        ref.append(float(kpi['ref']))
        refp.append(kpi['refPartner'])

    return {"kpis":kpis,"ref":ref,"refPartner":refp,"cat":cat,"catMaxName":catMaxName}        
 
def formatAxis(axis,kpi):
    return axis+ ' ('+kpi['refPartner']+')'