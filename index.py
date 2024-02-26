from ui import setUI
import streamlit as st
from snowflake.snowpark.files import SnowflakeFile
from PyPDF2 import PdfFileReader
from io import BytesIO
import pandas as pd
import lib
import os
import time
import random
from datetime import datetime
from snowflake.snowpark.types import StringType
from snowflake.snowpark.types import StringType, StructField, StructType
from langchain.text_splitter import RecursiveCharacterTextSplitter

st.set_page_config(layout='wide',initial_sidebar_state='collapsed')
session=lib.getSession()

def uploadPDF(file):
    with open(os.path.join("temp",file.name),"wb") as f: 
      f.write(file.getbuffer())         
    session.use_database('CORTEX_APP_DB')
    session.use_schema("CORTEX_APP_SC")
    put_result = session.file.put("temp/"+file.name, "@INPUT_FILE", source_compression="NONE",auto_compress=False)

def listFiles(file):
    session.use_database('CORTEX_APP_DB')
    session.use_schema("CORTEX_APP_SC")
    r= len(session.sql(f'''
        select RELATIVE_PATH  from directory(@INPUT_FILE) where RELATIVE_PATH='{file}';
    ''').collect())>0
    session.sql("ALTER STAGE INPUT_FILE REFRESH").collect()
    return r
    # if r==False:
    #       time.sleep(1)
    #       listFiles(file)
    #       stats.write('WAITING... ' + datetime.now().strftime("%H:%M:%S")) 
    # else:
    #     return True            
    # return len(session.sql(f'''
    # LIST @INPUT_FILE PATTERN='.*{file}.*'
    # ''').collect())>0

def checkUDFExist(name):
    return len(session.sql("SHOW USER FUNCTIONS LIKE '%"+name+"%' ").collect())

class chunker:
    def process(self,text):        
        text_raw=[]
        text_raw.append(text) 
        text_splitter = RecursiveCharacterTextSplitter(
            separators = ["\n"], 
            chunk_size = 4000, 
            chunk_overlap  = 200, 
            length_function = len,
            add_start_index = True 
        )
        chunks = text_splitter.create_documents(text_raw)
        df = pd.DataFrame(chunks, columns=['chunks','meta'])
        yield from df.itertuples(index=False, name=None)

def readpdf(file_path):
    whole_text = ""
    with SnowflakeFile.open(file_path, 'rb') as file:
        f = BytesIO(file.readall())
        pdf_reader = PdfFileReader(f)
        whole_text = ""
        for page in pdf_reader.pages:
            whole_text += page.extract_text()
    return whole_text

def registerUDF():
    session.use_database('CORTEX_APP_DB')
    session.use_schema("CORTEX_APP_SC")
    session.udf.register(
        func = readpdf
        , return_type = StringType()
        , input_types = [StringType()]
        , is_permanent = True
        , name = 'GET_PDF_TEXT'
        , replace = True
        , packages=['snowflake-snowpark-python','pypdf2']
        , stage_location = 'CORTEX_APP_DB.CORTEX_APP_SC.INPUT_FILE')
    schema = StructType([
        StructField("chunk", StringType()),
        StructField("meta", StringType()),
    ])
    session.udtf.register( 
        handler = chunker,
        output_schema= schema, 
        input_types = [StringType()] , 
        is_permanent = True , 
        name = 'CHUNK_TEXT' , 
        replace = True , 
        packages=['pandas','langchain'], 
        stage_location = 'CORTEX_APP_DB.CORTEX_APP_SC.INPUT_FILE')

def chunktable():
    session.use_database('CORTEX_APP_DB')
    session.use_schema("CORTEX_APP_SC")
    session.sql(f'''
        CREATE OR REPLACE TABLE CHUNK_TEXT AS
            SELECT
            relative_path,
            func.*
        FROM raw_text AS raw,
            TABLE(CHUNK_TEXT(raw_text)) as func;
    ''').collect()

def file2table():
    session.use_database('CORTEX_APP_DB')
    session.use_schema("CORTEX_APP_SC")
    session.sql(f'''
    CREATE OR REPLACE TABLE RAW_TEXT AS
        SELECT
            relative_path
            , file_url
            , GET_PDF_TEXT(build_scoped_file_url(@INPUT_FILE, relative_path)) as raw_text
        from directory(@INPUT_FILE);
    ''').collect()

def vectorize():
    session.use_database('CORTEX_APP_DB')
    session.use_schema("CORTEX_APP_SC")
    session.sql(f'''
    CREATE OR REPLACE TABLE VECTOR_STORE AS
        SELECT
            RELATIVE_PATH as EPISODE_NAME,
            CHUNK AS CHUNK,
            snowflake.cortex.embed_text('e5-base-v2', chunk) as chunk_embedding
        FROM CHUNK_TEXT;
    ''').collect()    


def prompt(text,rag=True):
    text=text.replace("'",' ')
    session.use_database('CORTEX_APP_DB')
    session.use_schema("CORTEX_APP_SC")
    if rag==True:
        return session.sql(f'''
        SELECT snowflake.cortex.complete(
        'llama2-70b-chat', 
        CONCAT( 
            'Answer the question based on the context. Be concise.','Context: ',
            (
                SELECT chunk FROM VECTOR_STORE 
                ORDER BY vector_l2_distance(
                snowflake.cortex.embed_text('e5-base-v2', 
                '{text}'
                ), chunk_embedding
                ) LIMIT 2
            ),
            'Question: ', 
            '{text}',
            'Answer: '
            )
        ) 
        ''') 
    else:
        return session.sql(f'''
        SELECT snowflake.cortex.complete(
        'llama2-70b-chat', 
        CONCAT( 
            'Answer the question. Be concise.',
            'Question: ', 
            '{text}',
            'Answer: '
            )
        ) 
        ''')       

def chat_actions(ans,res):
    st.session_state["chat_history"].append(
        {"role": "user", "content": ans},
    )

    st.session_state["chat_history"].append(
        {
            "role": "ai",
            "content":res,
        },
    )

def initialize(pc):
    # setUI()
    if 'init' not in st.session_state:
        res=checkUDFExist("CHUNK_TEXT")
        if res==0:
            pc.info("INSTALLING UDF...")
            registerUDF()
            pc.success("UDF SUCCESSFULY INSTALLED!")
        else:
            pc.success("UDF ALREADY INSTALLED!")
        time.sleep(4)
        pc.empty()
        st.session_state['init']=True    

def main():
    st.image('img/both.png',use_column_width='always')

    stats=st.empty()
    initialize(stats)
    up=st.file_uploader('Upload a PDF to augment LLM...',['pdf'])
    if up is not None and st.session_state.get('curfile')!=up.name:
        st.session_state['curfile']=up.name
        st.session_state['newfile']=True
    else:
        st.session_state['newfile']=False   
    if up is not None:
        if st.session_state['newfile']==True :  
            uploadPDF(up)
            stats.success("File Uploaded...")
            listFiles(up.name)
            file2table()
            stats.success("Text Extracted...")
            chunktable()
            stats.success("Chunked...")
            vectorize()
            stats.success("Vectorized...")
            time.sleep(2)
            stats.empty()  
    adv=st.checkbox("Use RAG?",value=True)  
    if adv==True: pre="RAG ACTIVATED" 
    else: pre="RAG DEACTIVATED"

    if "chat_history" not in st.session_state or len(st.session_state.get('chat_history'))>6:
        st.session_state["chat_history"] = []

    prpt=st.chat_input("Question:")  

    for i in st.session_state["chat_history"]:
        with st.chat_message(name=i["role"]):
            st.write(i["content"])   

    if prpt:
        with st.chat_message("user"):
            st.write(prpt) 
        res=prompt(prpt,adv).to_pandas().iloc[:, 0][0]
        with st.chat_message("ai"):    
            st.write(f'''({pre}) {res} ''')
        chat_actions(prpt,f'''({pre}) {res} ''')  

main()

























# do you have an example of customer using snowflake with Keboola in hospitality?

