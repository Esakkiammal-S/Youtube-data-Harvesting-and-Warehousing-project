#ESAKKIAMMAL DATA SCIENCE PROJECT

#Python packages import - YouTube Data Harvesting Project 
from googleapiclient.discovery import build
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
import streamlit as st
#from pprint import pprint


# Get Channel Details
def channel_details(youtube,chan_detail):

    
    request = youtube.search().list(
        part='snippet',
        maxResults=15,
        q=chan_detail
    )
    response = request.execute()
    # pprint(response)
    try:
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

    except:
        st.warning('Enter a Valid Channel Name')


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
               # part='snippet',
                part='contentDetails',
                playlistId=channel_playlistId,
                maxResults=50,
                pageToken=next_page_token)
            response1 = request1.execute()
            # pprint(response1)

            for i in range(len(response1['items'])):

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
                              "v_likecount": item['statistics']['likeCount'],
                              "v_duration":item['contentDetails']['duration']}

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
                                "comment_author": data['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                "comment_publishedAt": data['snippet']['topLevelComment']['snippet']['publishedAt']}

                commentDetails.append(comment_view)
    except:
        pass

    return commentDetails


# Youtube API Connection
api_key="AIzaSyAfT9POq5OU1kM1AdhRDkwYUMa5AK3sNq0"
api_service_name="youtube"
api_version="v3"
youtube=build(api_service_name,api_version,developerKey=api_key)


# MySQL Connection Thread
mydb = mysql.connector.connect(host="localhost", user="wonder", password="Esakki2024", database="youtube_collection",port=3306,charset='utf8mb4')
mycursor = mydb.cursor(buffered=True)

#Create sqlalchemy engine to connect to mysql database
engine = create_engine("mysql+mysqlconnector://{user}:{password}@localhost/{mydatabase}"
                       .format(user="wonder", password="Esakki2024", mydatabase="youtube_collection"))

# Create channel Table in SQL
mycursor.execute('''create table IF NOT EXISTS channel_details (channel_id VARCHAR(255) PRIMARY KEY,channel_name 
                 VARCHAR(255),channel_publishedAt VARCHAR(255),channel_subscribercount INT,channel_viewcount INT,
                 channel_videocount INT, channel_playlistId VARCHAR(255))''')

# Create video Table in SQL
mycursor.execute('''create table IF NOT EXISTS video_details_pj (v_id VARCHAR(255) PRIMARY KEY,channel_name VARCHAR(255),
                  channel_id VARCHAR(255),v_title VARCHAR(255),v_publishedAt VARCHAR(50),v_viewCount BIGINT, v_likeCount BIGINT,
                  v_commentCount BIGINT,v_duration VARCHAR (30), v_Thumbnail VARCHAR(200))''')

#Create comment Table in SQL
mycursor.execute('''create table IF NOT EXISTS comment_details_pj (comment_id VARCHAR(255) PRIMARY KEY,
                 v_id VARCHAR(255),comment_text TEXT,comment_author VARCHAR(255), 
                 comment_publishedAt VARCHAR(50))''')

#home page Title
st.title(':green[YOUTUBE DATA HARVESTING AND WAREHOUSING PROJECT]')
st.write(':blue[ESAKKIAMMAL S]')

## Data Extraction title
st.subheader(':red[YouTube Data Extraction]')

# User Input - Channel Name through Streamlit App
Enter_channel =st.text_input("Enter your channel Name:")
if st.button("submit"):
    st.write(Enter_channel)


#DATA EXTRACTION

try:
    #Get channel details and convert to pandas DataFrame
    channel = channel_details(youtube,Enter_channel)
    channel_df = pd.DataFrame(channel, index=[0])

    #Get video details and convert to pandas DataFrame
    videoDetails = video_id_details(youtube, channel['channel_playlistId'])
    channel_videos = channel_videodetails(youtube, videoDetails)
    video_df = pd.DataFrame(channel_videos)

    # Get comment details and convert to pandas DataFrame
    video_comment = comment_details(youtube, videoDetails)
    comment_df = pd.DataFrame( video_comment)

    st.success('Channel,video,comment Data Successfully Extracted')
except KeyError:
    st.warning('Enter a Valid Channel Name')

#  DATA DISPLAY

#Data tables view title
st.subheader(':red[YouTube Data Tables View ]')

#Show the data extracted for the input channel Name
if st.button('Channel'):
    st.write('Channel Data Table')
    channel_df
    st.success('Channel Data Successfully Extracted')

if st.button('Videos'):
    st.write('Video Data Table')
    video_df
    st.success('Video Data Successfully Extracted')

if st.button('Comments'):
    st.write('Comment Data Table')
    comment_df
    st.success('Comment Data Successfully Extracted')

# INSERT DATA INTO SQL DB
st.subheader(':red[YouTube Data Insert into SQL]')

#Insert the youtube data into SQL Database
if st.button('Insert Data into SQL Database'):
    
    mycursor.execute('select channel_name from youtube_collection.channel_details where channel_name=%s', [Enter_channel])
    mydb.commit()
    result = mycursor.fetchall()

    if result:
        st.success('Channel Name already available in Database')
    else:
        channel_df.to_sql('channel_details', con=engine, if_exists='append', index=False)
        video_df.to_sql('video_details_pj', con=engine, if_exists='append', index=False)
        comment_df.to_sql('comment_details_pj', con=engine, if_exists='append', index=False)
        mydb.commit()
        st.success('All data extracted for channel_details, video_details, comment_details are inserted into SQL Database Successfully')

#10 QUERY DATA FROM DATABASE
st.subheader(':red[YouTube Data from Database based on 10 Queries]')

# Below given queries to view data from DataBase

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

st.subheader('Choose Your Queries')
sql_query= st.selectbox('Choose the following Queries ',(q1,q2,q3,q4,q5,q6,q7,q8,q9,q10),
                           index=None, placeholder='Select your query')

if sql_query == q1:

    query1='''select v_title as videoName,channel_name as channelname from video_details_pj'''
    mycursor.execute(query1)
    mydb.commit()
    t1 = mycursor.fetchall()
    df1 = pd.DataFrame(t1, columns=["Video Name", "Channel Name"])
    st.write(df1)

elif sql_query == q2:

    query2 ='''select channel_name as channelname, channel_videocount as videocount from channel_details 
                  order by channel_videocount desc'''
    mycursor.execute(query2)
    mydb.commit()
    t2 = mycursor.fetchall()
    df2 = pd.DataFrame(t2, columns=["channel name", "video count"])
    st.write(df2)

elif sql_query == q3:
    query3 = '''select v_viewcount as viewcount,channel_name as channelname,v_title as videoname from video_details_pj
                  where v_viewcount is not null order by v_viewcount desc limit 10'''
    mycursor.execute(query3)
    mydb.commit()
    t3 = mycursor.fetchall()
    df3 = pd.DataFrame(t3, columns=["viewcount","channelname","video title"])
    st.write(df3)

elif sql_query == q4:
    query4 = '''select v_commentcount as commentcount,v_title as video_title from video_details_pj where 
                  v_commentcount is not null'''
    mycursor.execute(query4)
    mydb.commit()
    t4 = mycursor.fetchall()
    df4 = pd.DataFrame(t4, columns=["comment count","video title"])
    st.write(df4)

elif sql_query == q5:
    query5 = '''select v_title as video_title,channel_name as channelname,v_likecount as like_count from video_details_pj
                   where v_likecount is not null order by v_likecount desc'''
    mycursor.execute(query5)
    mydb.commit()
    t5 = mycursor.fetchall()
    df5 = pd.DataFrame(t5, columns=["video title","channelname","likecount"])
    st.write(df5)

elif sql_query == q6:
    query6 = '''select v_likecount as like_count,v_title as video_title from video_details_pj'''
    mycursor.execute(query6)
    mydb.commit()
    t6 = mycursor.fetchall()
    df6 = pd.DataFrame(t6, columns=["likecount","video title"])
    st.write(df6)

elif sql_query == q7:
    query7 = '''select channel_name as channelname,channel_viewcount as viewcount from channel_details'''
    mycursor.execute(query7)
    mydb.commit()
    t7 = mycursor.fetchall()
    df7 = pd.DataFrame(t7, columns=["channelname","viewcount"])
    st.write(df7)

elif sql_query == q8:
    query8 = '''select v_title as video_title,v_publishedAt as published_date,channel_name as channelname from video_details_pj
                  where extract(year from v_publishedAt)=2022'''
    mycursor.execute(query8)
    mydb.commit()
    t8 = mycursor.fetchall()
    df8 = pd.DataFrame(t8, columns=["Video Name", "Video Release Date", "Channel Name"])
    st.write(df8)

elif sql_query == q9:
    query9 = '''select channel_name as channelname,time_format(SEC_TO_TIME(AVG(TIME_TO_SEC(v_duration))),'%H:%i:%s') as average_duration from video_details_pj
                   group by channel_name'''
    mycursor.execute(query9)
    mydb.commit()
    t9= mycursor.fetchall()
    df9 = pd.DataFrame(t9, columns=["channelname","averageduration"])
    sql_q9 = []

    for index, row in df9.iterrows():
        channel_Name = row["channelname"]
        Avg_duration = row["averageduration"]
        Avg_duration_str = str(Avg_duration)
        sql_q9.append(dict(channelName=channel_Name, averageduration=Avg_duration_str))
    df = pd.DataFrame(sql_q9)
    st.write(df)
elif sql_query == q10:
    query10 = '''select v_title as video_title,channel_name as channelname,v_commentcount as comment_count from video_details_pj
                    where v_commentcount is not null order by v_commentcount desc'''
    mycursor.execute(query10)
    mydb.commit()
    t10 = mycursor.fetchall()
    df10 = pd.DataFrame(t10, columns=["video title","channelname","commentcount"])
    st.write(df10)



st.header(':blue[YouTube Data Harvesting and Warehousing Project]')
st.subheader('SKILLS HIGHLIGHT')
st.caption('Youtube API Integration')
st.caption('Python Scripting')
st.caption('Pandas DataFrame')
st.caption('Data Extraction')
st.caption('Data Management using SQL')
st.caption('Streamlit Web Application')
