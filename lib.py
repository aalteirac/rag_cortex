import configparser
from snowflake.snowpark import Session
import streamlit as st
import pandas as pd
import json
import time
import os
from io import StringIO


def init(dbname,scname,stname,st,pc):
    if 'init' not in st.session_state:
        pc.info("CHECKING DATABASE...")
        if checkDB(dbname)==False:
            pc.info("CREATING DATABASE...")
            getSession().sql("CREATE DATABASE "+dbname).collect()
            pc.success("DATABASE CREATED...")
        pc.info("CHECKING SCHEMA...")
        if checkSchema(dbname,scname)==False:
            pc.info("CREATING SCHEMA...")
            getSession().sql("CREATE SCHEMA "+dbname+"."+scname).collect()
            pc.success("SCHEMA CREATED...")
        pc.info("CHECKING STAGE...")   
        if checkStage(dbname,scname,stname)==False:
            pc.info("CREATING STAGE...")
            getSession().sql("CREATE STAGE "+dbname+"."+scname+"."+stname +" DIRECTORY=(ENABLE=TRUE)").collect()
            pc.success("STAGE CREATED...")
        if 'init' not in st.session_state:
            res=checkUDFExist(dbname,scname,"CHUNK_TEXT")
            return res
        
def checkStage(dbname,scname,stname):
    session=getSession()
    return len(session.sql("SHOW STAGES LIKE '%"+stname+"%' in SCHEMA " +dbname+"."+scname).collect())>0

def checkDB(dbname):
    session=getSession()
    return len(session.sql("SHOW DATABASES LIKE '%"+dbname+"%' ").collect())>0

def checkSchema(dbname,scname):
    session=getSession()
    return len(session.sql("SHOW SCHEMAS LIKE '%"+scname+"%' in DATABASE "+ dbname).collect())>0

def checkUDFExist(dbname,scname,funcname):
    session=getSession()
    return len(session.sql("SHOW USER FUNCTIONS LIKE '%"+funcname+"%' in SCHEMA " +dbname+"."+scname).collect())

def getSession():
    config = configparser.ConfigParser()
    config.read("secrets.toml")
    session = Session.builder.configs(dict({"user":st.secrets['user'],"password":st.secrets['password'],'account':st.secrets['account'],"role":st.secrets['role'],"warehouse":st.secrets['warehouse']})).create() 
    return session
