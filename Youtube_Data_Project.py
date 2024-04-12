#packages import
from googleapiclient.discovery import build
import pandas as pd
from pprint import pprint
import sqlalchemy
import mysql.connector
import streamlit as st


#streamlit
st.title("Youtube Data Harvesting And Warehousing Project")
st.header("Welcome User")



# Get Channel Details

def channel_details(youtube,chan_detail):


    youtube = build(api_service_name, api_version, developerKey=api_key)
    request = youtube.search().list(
        part='snippet',
        maxResults=15,
        q=chan_detail
    )
    response = request.execute()
    # pprint(response)

    channel_id = response['items'][0]['snippet']['channelId']
    request1 = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response1 = request1.execute()

    # pprint(response1)
    channel_id = response1['items'][0]['id']
    channel_name = response1['items'][0]['snippet']['title']
    channel_publishedAt = response1['items'][0]['snippet']['publishedAt']
    channel_subscribercount = response1['items'][0]['statistics']['subscriberCount']
    channel_videocount = response1['items'][0]['statistics']['videoCount']
    channel_viewcount = response1['items'][0]['statistics']['viewCount']
    channel_playlistId = response1['items'][0]['contentDetails']['relatedPlaylists']['uploads']


    return {"channel_id": channel_id,
            "channel_name": channel_name,
            "channel_publishedAt": channel_publishedAt,
            "channel_subscribercount": channel_subscribercount,
            "channel_videocount": channel_videocount,
            "channel_viewcount": channel_viewcount,
            "channel_playlistId": channel_playlistId}


# get video ids details
def video_id_details(youtube,channel_playlistId):

    video_ids = []
    next_page_token = None
    try:
        while True:
            request1 = youtube.playlistItems().list(
                part='contentDetails',
                playlistId=channel_playlistId,
                maxResults=50,
                pageToken=next_page_token)
            response1 = request1.execute()
            # pprint(response1)

            for i in range(len(response1['items'])):
                #video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
                video_ids.append(response1['items'][i]['contentDetails']['videoId'])
            next_page_token = response1['nextPageToken']
    except:
        pass
    return video_ids




# video details
def channel_videodetails(youtube,videoDetails):
    videos_viewlist = []
    for video_id in videoDetails:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id)
        response = request.execute()
        # pprint(videoDetails)

        try:

            for item in response['items']:
                video_view = {"channel_name": item['snippet']['channelTitle'],
                              "channel_id": item['snippet']['channelId'],
                              "v_id": item['id'],
                              "v_title": item['snippet']['title'],
                              "v_Thumbnail": item['snippet']['thumbnails']['default']['url'],
                              "v_publishedAt": item['snippet']['publishedAt'],
                              "v_viewcount": item['statistics']['viewCount'],
                              "v_commentcount": item['statistics']['commentCount'],
                              "v_likecount": item['statistics']['likeCount']}

            videos_viewlist.append(video_view)
            # pprint(videos)

        except:
            pass


    return videos_viewlist




# comment Details
def comment_details(youtube,video_ids):
    commentDetails = []
    try:
        for i in video_ids:
            request = youtube.commentThreads().list(part='snippet',
                                                    videoId=i,
                                                    maxResults=50
                                                    )
            response = request.execute()
            for data in response['items']:
                comment_view = {"comment_id": data['snippet']['topLevelComment']['id'],
                                "v_id": data['snippet']['topLevelComment']['snippet']['videoId'],
                                "comment_Text": data['snippet']['topLevelComment']['snippet']['textDisplay'],
                                "comment_author": data['snippet']['topLevelComment']['snippet'][
                                    'authorDisplayName'],
                                "comment_publishedAt": data['snippet']['topLevelComment']['snippet']['publishedAt']}

                commentDetails.append(comment_view)
    except:
        pass

    return commentDetails




def channel_info(youtube,channel_name):

    #Get channel_details

    channel = channel_details(youtube,channel_name)
    #Get channel_videoId_details
    videoDetails = video_id_details(youtube,channel['channel_playlistId'])
    #channel_video_Details
    channel_videos=channel_videodetails(youtube,videoDetails)
    #comment_details
    video_comment=comment_details(youtube,videoDetails)

    data={"channel_Details":channel,
          "video_Detail": channel_videos,
          "comment_Detail":video_comment}

    return data

#Get Api_key Details
api_key="AIzaSyDhk7V54ypMRpPg_SUIR1ChETQESPjPx9M"
api_service_name="youtube"
api_version="v3"
youtube=build(api_service_name,api_version,developerKey=api_key)




#streamlit get input from user

Enter_channel =st.text_input("Enter your channel:")
main_info=channel_info(youtube,Enter_channel)
if st.button("submit"):
    st.write(Enter_channel)


    con = mysql.connector.connect( user='wonder',
                                password='Esakki2024',
                                host='localhost',
                                database='youtube_collection',
                                port=3306,
                                charset='utf8mb4'
                                )


    cursor = con.cursor()
    cursor.execute("select * from channel_details")
    ch_sql=cursor.fetchall()
    ch_Name = []
    for check_channel in ch_sql:

       ch_Name.append(check_channel["channel_name"])

    if Enter_channel in ch_Name:
        st.success("channel name is already exists")
    else:
        main_info=channel_info(youtube,Enter_channel)
        st.success(main_info)


#streamlit insert data to sql
    if st.button("insert data to sql"):
        Table_sql=db_tables()
        st.success(Table_sql)

    #streamlit show tables
    show_tables=st.selectbox("select table name",("channel_table"," video_table","comment_table"))

    if show_tables=="channel_table":
        channel_table_show()

    elif show_tables==" video_table":
        video_table_show()

    elif show_tables=="comment_table":
        comment_table_show()

# 10  sql queries

con = mysql.connector.connect(
user='wonder',
password='Esakki2024',
host='localhost',
database='youtube_collection',
port=3306,
charset='utf8mb4'
)


cursor = con.cursor()
q1 = '1. What are the names of all the videos and their corresponding channels?'
q2 = '2. Which channels have the most number of videos, and how many videos do they have?'
q3 = '3. What are the top 10 most viewed videos and their respective channels?'
q4 = '4. How many comments were made on each video, and what are their corresponding video names?'
q5 = '5. Which videos have the highest number of likes, and what are their corresponding channel names?'
q6 = '6. What is the total number of likes for each video, and what are their corresponding video names?'
q7 = '7. What is the total number of views for each channel, and what are their corresponding channel names?'
q8 = '8. What are the names of all the channels that have published videos in the year 2022?'
q9 = '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?'
q10 = '10. Which videos have the highest number of comments, and what are their corresponding channel names?'

question=st.selectbox("select your question",(q1,q2,q3,q4,q5,q6,q7,q8,q9,q10))

if question == q1:
    sql_query = "SELECT vd.v_title,ch.channel_name FROM video_details_pj vd INNER JOIN channel_details ch ON vd.channel_id = ch.channel_id;"
    data, columns = sql_query.fetchall()
    df = pd.DataFrame(data, columns=columns)
    st.dataframe(df)

elif question == q2:
    sql_query = "SELECT ch.channel_name, COUNT(vd.v_id) AS video_count FROM channel_details ch INNER JOIN video_details_pj vd ON ch.channel_id = vd.channel_id GROUP BY ch.channel_name ORDER BY video_count DESC LIMIT 1;"
    data, columns = sql_query.fetchall()
    df = pd.DataFrame(data, columns=columns)
    st.dataframe(df)
elif question == q3:
    sql_query = "SELECT ch.channel_name,vd.v_title, vd.v_viewCount FROM video_details_pj vd JOIN channel_details ch ON vd.channel_id = ch.channel_id ORDER BY vd.viewCount DESC LIMIT 10;"
    data, columns = sql_query.fetchall()
    df = pd.DataFrame(data, columns=columns)
    st.dataframe(df)

elif question == q4:
    sql_query = "SELECT vd.v_title, COUNT(cmt.comment_id) AS comment_count FROM video_details_pj vd LEFT JOIN comment_details_pj cmt ON vd.v_id = cmt.v_id GROUP BY vd.v_title;"
    data, columns = sql_query.fetchall()
    df = pd.DataFrame(data, columns=columns)
    st.dataframe(df)

elif question == q5:
    sql_query = "SELECT vd.v_title, ch.channel_name, vd.v_likeCount as Likes_count FROM video_details_pj vd INNER JOIN channel_details ch ON vd.channel_id = ch.channel_id ORDER BY vd.v_likeCount DESC LIMIT 10;"
    data, columns = sql_query.fetchall()
    df = pd.DataFrame(data, columns=columns)
    st.dataframe(df)

elif question == q6:
    sql_query = "SELECT vd.v_title, SUM(vd.v_likeCount) AS Total_likes FROM video_details_pj vd GROUP BY vd.v_title;"
    data, columns = sql_query.fetchall()
    df = pd.DataFrame(data, columns=columns)
    st.dataframe(df)

elif question == q7:
    sql_query = "SELECT ch.channel_name, SUM(vd.viewCount) AS total_views FROM channel_details ch INNER JOIN video_details_pj vd ON ch.channel_id = vd.channel_id GROUP BY ch.channel_name;"
    data, columns = sql_query.fetchall()
    df = pd.DataFrame(data, columns=columns)
    st.dataframe(df)

elif question == q8:
    sql_query = "SELECT DISTINCT ch.channel_name FROM channel_details ch INNER JOIN video_details_pj vd ON ch.channel_id = vd.channel_id WHERE YEAR(vd.v_publishedAt) = 2022;"
    data, columns = sql_query.fetchall()
    df = pd.DataFrame(data, columns=columns)
    st.dataframe(df)
elif question == q9:
    st.write("Duration not defined")
elif question == q10:
    sql_query = "SELECT vd.v_title, ch.channel_name, COUNT(cmt.comment_id) AS comment_count FROM video_details_pj vd INNER JOIN channel_details ch ON vd.channel_id = ch.channel_id LEFT JOIN comment_details_pj cmt ON vd.v_id = cmt.v_id GROUP BY vd.v_title, ch.channel_name ORDER BY comment_count DESC LIMIT 1;"
    data, columns = sql_query.fetchall()
    df = pd.DataFrame(data, columns=columns)
    st.dataframe(df)


#streamlit table function
def channel_table_show():
    df1=st.dataframe(main_info["channel_Detail"],index=[0])

    return channel_df

def video_table_show():
    df2=st.dataframe(main_info["video_Detail"])

    return video_df

def comment_table_show():

    df3=st.dataframe(main_info["comment_Detail"])

    return comment_df

channel_df=pd.DataFrame(main_info["channel_Details"],index=[0])
video_df=pd.DataFrame(main_info["video_Details"])
comment_df=st.dataframe(main_info["comment_Details"])


#Table creation for channel,video,comment
def channel_table():

    con = mysql.connector.connect(
    user='wonder',
    password='Esakki2024',
    host='localhost',
    database='youtube_collection',
    port=3306,
    charset='utf8mb4'
    )


    cursor = con.cursor()

    # drop_query='''drop table if exists channel_details'''
    # cursor.execute(drop_query)
    #
    # sql_query='''CREATE TABLE IF NOT EXISTS channel_details( channel_id VARCHAR(80) PRIMARY KEY,
    #                                                         channel_name VARCHAR(100),
    #                                                         channel_publishedAt VARCHAR(50) ,
    #                                                         channel_subscribercount BIGINT,
    #                                                         channel_videocount INTEGER,
    #                                                         channel_viewcount  BIGINT,
    #                                                         channel_playlistId  VARCHAR(80)
    #                                                         )'''
    #
    # cursor.execute(sql_query)

    for index,row in channel_df.iterrows():
        insert_query='''insert into channel_details(channel_id ,
                                        channel_name ,
                                        channel_publishedAt ,
                                        channel_subscribercount,
                                        channel_videocount ,
                                        channel_viewcount  ,
                                        channel_playlistId)
                                        values(%s,%s,%s,%s,%s,%s,%s)'''


        values=(row['channel_id'],
            row['channel_name'],
            row['channel_publishedAt'],
            row['channel_subscribercount'],
            row['channel_videocount'],
            row['channel_viewcount'],
            row['channel_playlistId'])



        cursor.execute(insert_query,values)
        con.commit()

def video_table():

    con = mysql.connector.connect(
     user='wonder',
     password='Esakki2024',
     host='localhost',
     database='youtube_collection',
     port=3306,
     charset='utf8mb4'
    )

    cursor = con.cursor()

    # drop_query='''drop table if exists video_details_pj'''
    # cursor.execute(drop_query)
    #
    # video_query='''CREATE TABLE IF NOT EXISTS video_details_pj( v_id VARCHAR(50) PRIMARY KEY,
    #                                                         channel_name VARCHAR(100),
    #                                                         channel_id VARCHAR(80),
    #                                                         v_title Text,
    #                                                         v_publishedAt VARCHAR(50),
    #                                                         v_viewCount BIGINT,
    #                                                         v_likeCount BIGINT,
    #                                                         v_commentCount BIGINT,
    #                                                         v_Thumbnail VARCHAR(200)
    #                                                         )
    # cursor.execute(video_query)
    #

    for index,row in video_df.iterrows():

        insert_query='''insert into video_details_pj(v_id ,
                                                channel_name,
                                                channel_id,
                                                v_title,
                                                v_publishedAt,
                                                v_viewcount,
                                                v_likecount,
                                                v_commentcount,
                                                v_Thumbnail
                                                )
                                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

        values=(row['v_id'],
                row['channel_name'],
                row['channel_id'],
                row['v_title'],
                row['v_publishedAt'],
                row['v_viewcount'],
                row['v_likecount'],
                row['v_commentcount'],
                row['v_Thumbnail'])

        cursor.execute(insert_query,values)
        con.commit()


def comment_table():
    con = mysql.connector.connect(
        user='wonder',
        password='Esakki2024',
        host='localhost',
        database='youtube_collection',
        port=3306,
        charset='utf8mb4'
    )

    cursor = con.cursor()

    # drop_query='''drop table if exists comment_details_pj'''
    # cursor.execute(drop_query)
    #
    # comment_query='''CREATE TABLE IF NOT EXISTS comment_details_pj( comment_id VARCHAR(100) PRIMARY KEY,
    #                                                             v_id VARCHAR(50),
    #                                                             comment_Text TEXT,
    #                                                             comment_author VARCHAR(150),
    #                                                             comment_publishedAt VARCHAR(50)
    #                                                             )'''
    #
    # cursor.execute(comment_query)

    for index,row in comment_df.iterrows():
        insert_query='''insert into comment_details_pj (comment_id ,
                                                     v_id ,
                                                     comment_Text ,
                                                     comment_author ,
                                                     comment_publishedAt 
                                                     )
                                            values(%s,%s,%s,%s,%s)'''


        values=(row['comment_id'],
                row['v_id'],
                row['comment_Text'],
                row['comment_author'],
                row['comment_publishedAt'])


        cursor.execute(insert_query,values)
        con.commit()

#Tables
def db_tables():
    channel_table()
    video_table()
    comment_table()
    return "Tables created successfully"

Tables_sql=db_tables()

