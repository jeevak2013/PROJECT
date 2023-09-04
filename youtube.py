from googleapiclient.discovery import build
from pymongo import MongoClient
import sqlite3
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import concurrent.futures
import os
import time
import threading

# Create a lock to synchronize access to the SQLite database
db_lock = threading.Lock()

API_SERVICE_NAME = 'youtube'
API_VERSION = 'v3'
API_KEY = 'AIzaSyAchxjMflU8o1BSlpeSCD3yd-k95qrWgmU'
mongodb_connection_string = 'mongodb+srv://guvi:guvi1234@guvi.kv1fpcn.mongodb.net/?retryWrites=true&w=majority'
channel_titles = []

def get_authenticated_service():
    return build(API_SERVICE_NAME,API_VERSION,developerKey=API_KEY)

def get_channel_id_by_name(channel_title):
  service = get_authenticated_service()

  variations = [channel_title.lower(), channel_title.upper(), channel_title.capitalize()]

  for variation in variations:
    search_response = service.search().list(
        part = 'id',
        q = variation,
        type = 'channel',
        maxResults = 1
    ).execute()

    if 'items' in search_response:
      channel_id = search_response['items'][0]['id']['channelId']
      return channel_id

  return None


def get_channel_details(channel_id):
  service = get_authenticated_service()

  channel_response = service.channels().list(
      part = 'snippet,contentDetails,statistics',
      id = channel_id
  ).execute()

  channel_details = dict(
      channel_id = channel_id,
      channel_name = channel_response['items'][0]['snippet']['title'],
      Description = channel_response['items'][0]['snippet']['description'],
      playlist_id = channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
      subscribers = channel_response['items'][0]['statistics']['subscriberCount'],
      views = channel_response['items'][0]['statistics']['viewCount'],
      Total_videos = channel_response['items'][0]['statistics']['videoCount'],
  )

  return channel_details

def get_playlist_details_and_video_id(playlist_id):
  service = get_authenticated_service()

  next_page_token = None
  video_ids = []
  while True:
    playlist_response = service.playlistItems().list(
        playlistId = playlist_id,
        part = 'snippet',
        maxResults = 50,
        pageToken  = next_page_token
    ).execute()

    for i in range(len(playlist_response['items'])):
      video_id = playlist_response['items'][i]['snippet']['resourceId']['videoId']
      video_ids.append(video_id)
    next_page_token = playlist_response.get('nextPageToken')

    if next_page_token is None:
      break

  playlist_info = {
      'playlist_id': playlist_id,
      'video_ids': video_ids
  }

  return playlist_info, video_ids

def get_video_details(video_ids):
  service = get_authenticated_service()

  video_data = []
  for i in range(0,len(video_ids),50):
    video_response = service.videos().list(
        part ='snippet,contentDetails,statistics',
        id = ','.join(video_ids[i:i+50])
    ).execute()

    for video in video_response['items']:
      video_details = dict(Channel_name = video['snippet']['channelTitle'],
                           Channel_id = video['snippet']['channelId'],
                           Video_id = video['id'],
                           Title = video['snippet']['title'],
                           Duration = video['contentDetails']['duration'],
                           Published_at = video['snippet']['publishedAt'],
                           View_count = video['statistics']['viewCount'],
                           Like_count = video['statistics'].get('likeCount',0),
                           Dislike_count = video['statistics'].get('dislikeCount',0)
                           )
      video_data.append(video_details)

  return video_data

def get_comment_details(video_ids):
  service = get_authenticated_service()

  comment_data = []
  
  try:
    for video_id in video_ids:
      next_page_token = None

      while True:
        comment_response = service.commentThreads().list(
            videoId = video_id,
            part = 'snippet,replies',
            maxResults = 100,
            pageToken  = next_page_token
        ).execute()
        
        for comment in comment_response['items']:
          comment_details = dict(Comment_id = comment['id'],
                                Video_id = comment['snippet']['videoId'],
                                Comment_text = comment['snippet']['topLevelComment']['snippet']['textDisplay'],
                                Comment_published_at = comment['snippet']['topLevelComment']['snippet']['publishedAt'],
                                )
          comment_data.append(comment_details)
        next_page_token = comment_response.get('nextPageToken')

        if next_page_token is None:
          break
  except Exception as e:
    print("Error fetching comment details:", str(e))

  return comment_data

def store_in_mongodb(combined_data,channel_name):
  client = MongoClient(mongodb_connection_string)
  db = client['youtube_data2']
  if channel_name not in db.list_collection_names():
    collection = db[channel_name]
    collection.insert_one(combined_data)
    client.close()

def create_tables(connection):
  cursor = connection.cursor()

  cursor.execute('''
          CREATE TABLE IF NOT EXISTS Channel(
            channel_id TEXT PRIMARY KEY,
            channel_name TEXT,
            channel_description TEXT,
            channel_subscribers INTEGER,
            channel_views INTEGER,
            channel_total_videos INTEGER
            )''')

  cursor.execute('''
          CREATE TABLE IF NOT EXISTS Playlist(
            playlist_id TEXT PRIMARY KEY,
            channel_id TEXT,
            FOREIGN KEY(channel_id) REFERENCES Channel(channel_id)
            )''')

  cursor.execute('''
          CREATE TABLE IF NOT EXISTS Video(
            video_id TEXT PRIMARY KEY,
            playlist_id TEXT,
            video_title TEXT,
            video_duration TEXT,
            published_at TEXT,
            view_count INTEGER,
            like_count INTEGER,
            FOREIGN KEY(playlist_id) REFERENCES Playlist(playlist_id)
          )''')

  cursor.execute('''
          CREATE TABLE IF NOT EXISTS Comment(
            comment_id TEXT PRIMARY KEY,
            video_id TEXT,
            comment_text TEXT,
            comment_published_at TEXT,
            FOREIGN KEY (video_id) REFERENCES Video(video_id)
          )''')

  connection.commit()

def migrate_channel_details(connection,channel_details):
  cursor = connection.cursor()
  
  cursor.execute('''
            INSERT OR REPLACE INTO Channel(channel_id, channel_name, channel_description, channel_subscribers, channel_views, channel_total_videos)
            VALUES (?,?,?,?,?,?)
            ''',(
            channel_details['channel_id'],
            channel_details['channel_name'],
            channel_details['Description'],
            int(channel_details['subscribers']),
            int(channel_details['views']),
            int(channel_details['Total_videos'])
            ))
  connection.commit()

def migrate_playlist_details(connection,playlist_id,channel_id):
  cursor = connection.cursor()

  cursor.execute('''
          INSERT OR REPLACE INTO Playlist(playlist_id, channel_id)
          VALUES (?,?)
          ''',(
          playlist_id,
          channel_id
          ))
  connection.commit()

def migrate_video_details(connection,video_details,playlist_id):
  cursor = connection.cursor()

  for video_detail in video_details:
    cursor.execute('''
            INSERT OR REPLACE INTO Video(video_id, playlist_id, video_title, video_duration, published_at, view_count, like_count)
            VALUES (?,?,?,?,?,?,?)
            ''',(
            video_detail['Video_id'],
            playlist_id,
            video_detail['Title'],
            video_detail['Duration'],
            video_detail['Published_at'],
            int(video_detail['View_count']),
            int(video_detail['Like_count'])
            ))

    connection.commit()

def migrate_comment_details(connection,comment_data):
  cursor = connection.cursor()

  for comment_detail in comment_data:
    cursor.execute('''
            INSERT OR REPLACE INTO Comment(comment_id, video_id, comment_text, comment_published_at)
            VALUES (?,?,?,?)
            ''',(
            comment_detail['Comment_id'],
            comment_detail['Video_id'],
            comment_detail['Comment_text'],
            comment_detail['Comment_published_at'],
            ))

    connection.commit()

def migrate_data_from_mongodb_to_sql(channel_title):
  try:
    channel_id = get_channel_id_by_name(channel_title)
    if channel_id:
      channel_details = get_channel_details(channel_id)
      channel_name = channel_details['channel_name']

      with db_lock:
        client = MongoClient(mongodb_connection_string)
        db = client['youtube_data2']
        collection = db[channel_name]

        data = collection.find_one({})

        if data:
          connection = sqlite3.connect('youtube_data2.db')
          migrate_channel_details(connection,data['channel_details'])
          print("migrated channel details")
          migrate_playlist_details(connection,data['playlist_details']['playlist_id'],data['channel_details']['channel_id'])
          print("migrated playlist details")
          migrate_video_details(connection,data['video_details'], data['playlist_details']['playlist_id'])
          print("migrated video details")
          migrate_comment_details(connection,data['comment_details'])
          print("migrated comment details")
          connection.close()
        client.close()
        return data
  except Exception as e:
    print("Exception occured: ", str(e))

def execute_query(connection,query):
  cursor = connection.cursor()
  cursor.execute(query)
  result = cursor.fetchall()
  return result  

def extract_and_store_channel_details(channel_title):
  
  channel_id = get_channel_id_by_name(channel_title)
  print(channel_id)
        
  if channel_id:

      channel_details = get_channel_details(channel_id)
      playlist_id = channel_details['playlist_id']
      channel_name = channel_details['channel_name']
      
      playlist_details,video_ids = get_playlist_details_and_video_id(playlist_id)
      video_details = get_video_details(video_ids)
      print(len(video_details))
      comment_data = get_comment_details(video_ids)
      print(len(comment_data))

      combined_data ={
                'channel_details' : channel_details,
                'playlist_details' : playlist_details,
                'video_details' : video_details,
                'comment_details' : comment_data
                }
      print("combined_data passed")
                
      store_in_mongodb(combined_data,channel_name)
      print("stored in mongodb")
   
  else:
    print(f"Channel'{channel_title}' notfound.")

def main():    
  global channel_titles
  st.markdown("""<style>.content {margin-top: 80px;}</style>""",unsafe_allow_html=True,)
  if st.sidebar.button("Extract and Store channel details"):
    # migration start time
    with st.spinner(f"Fetching and transforming data"):
      start_time = time.time()

      num_workers = os.cpu_count()
      with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(extract_and_store_channel_details, channel_title) for channel_title in channel_titles]

      # Wait for all submitted tasks to complete
      concurrent.futures.wait(futures)

      end_time = time.time()
      elapsed_time = end_time - start_time
      st.success(f"Fetch and transform data completed in {elapsed_time:.2f} seconds.")


  
  if st.sidebar.button("Transform Data to Query"):
    with st.spinner("Transforming data to SQL"):
      # migration start time
      start_time = time.time()
      connection = sqlite3.connect('youtube_data2.db')
      create_tables(connection)
      connection.close()

      num_workers = os.cpu_count()
      with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(migrate_data_from_mongodb_to_sql, channel_title) for channel_title in channel_titles]

      concurrent.futures.wait(futures)

      for future in concurrent.futures.as_completed(futures):
        try:
          future.result()
        except Exception as e:
          print("Exception occured: ", str(e))
      executor.shutdown()
      end_time = time.time()
      elapsed_time = end_time - start_time
      st.success(f"Data transformation completed in {elapsed_time:.2f} seconds.")

  
  st.markdown("""<style>.content {margin-top: 80px;}</style>""",unsafe_allow_html=True,)
  query_options = st.sidebar.radio("Select a Query",[
    "What are the names of all the videos and their corresponding channels?",
    "Which channels have the most number of videos, and how many videos do they have?",
    "What are the top 10 most viewed videos and their respective channels?",
    "How many comments were made on each video, and what are their corresponding video names?",
    "Which videos have the highest number of likes, and what are their corresponding channel names?",
    "What is the total number of likes for each video, and what are their corresponding video names?",
    "What is the total number of views for each channel, and what are their corresponding channel names?",
    "What are the names of all the channels that have published videos in the year 2022?",
    "What is the average duration of all videos in each channel, and what are their corresponding channel names?",
    "Which videos have the highest number of comments, and what are their corresponding channel names?"
    ])
  
  # SQL QUERY
  if st.sidebar.button("Query with Database"):
    connection = sqlite3.connect('youtube_data.db')
        
    if query_options == "What are the names of all the videos and their corresponding channels?":
      query1 = '''
          SELECT V.video_title, C.channel_name
          FROM Video V    
          JOIN Playlist P ON V.playlist_id = P.playlist_id
          JOIN Channel C ON P.channel_id = C.channel_id
          '''
      
      st.subheader("What are the names of all the videos and their corresponding channels?")
      df_result1 = pd.read_sql_query(query1, connection)
      st.dataframe(df_result1)

    elif query_options == "Which channels have the most number of videos, and how many videos do they have?":
      # channel with most number of videos and count- ch.nm>pl id>vid
      query2 = '''
          SELECT C.channel_name, COUNT(V.video_id) AS video_count
          FROM Channel C
          JOIN Playlist P ON C.channel_id = P.channel_id
          JOIN Video V ON P.playlist_id = V.playlist_id
          GROUP BY C.channel_name
          ORDER BY video_count DESC
          '''
      col1,col2 = st.columns([0.6, 0.4])

      with col1:
        st.subheader("Query Results")
        df_result2 = pd.read_sql_query(query2, connection)
        st.dataframe(df_result2)

      with col2:
        plt.figure()
        plt.bar(df_result2['channel_name'], df_result2['video_count'])
        plt.xticks(rotation=45, ha='right')
        plt.xlabel('Channel Name')
        plt.ylabel('Video count')
        plt.title("Videos per Channel")
        st.subheader("Chart")
        st.pyplot(plt)

    elif query_options == "What are the top 10 most viewed videos and their respective channels?":
      # TOP 10 most viewed videos and their channel channel
      query3 ='''
          SELECT V.video_title, C.channel_name, V.view_count
          FROM Video V
          JOIN Playlist P ON V.playlist_id = P.playlist_id
          JOIN Channel C ON P.channel_id = C.channel_id
          ORDER BY V.view_count DESC
          LIMIT 10
          '''
      df_result3 = pd.read_sql_query(query3, connection)
      st.subheader("What are the top 10 most viewed videos and their respective channels?")
      st.dataframe(df_result3)

    elif query_options == "How many comments were made on each video, and what are their corresponding video names?":
      # video name and comment count TOP 10
      query4 = '''
          SELECT V.video_title, COUNT(Com.comment_id) AS comment_count
          FROM Video V  --parent
          LEFT JOIN Comment Com ON V.video_id = Com.video_id
          GROUP BY V.video_title
          ORDER BY comment_count DESC
          LIMIT 10
          '''
      df_result4 = pd.read_sql_query(query4, connection)
      st.subheader("How many comments were made on each video, and what are their corresponding video names?")
      st.dataframe(df_result4.head())

    elif query_options == "Which videos have the highest number of likes, and what are their corresponding channel names?":
      # video highest likes and channel name
      query5 = '''
          SELECT V.video_title,V.like_count, C.channel_name
          FROM Video V
          JOIN Playlist P ON V.playlist_id = P.playlist_id
          JOIN Channel C ON P.channel_id = C.channel_id
          ORDER BY V.like_count DESC
          '''
      df_result5 = pd.read_sql_query(query5, connection)
      st.subheader("Which videos have the highest number of likes, and what are their corresponding channel names?")
      st.dataframe(df_result5)

    elif query_options == "What is the total number of likes for each video, and what are their corresponding video names?":
      # TOTAL like for videos
      query6 = '''
          SELECT V.video_title, SUM(V.like_count) AS total_likes
          FROM Video V
          GROUP BY V.Video_title
          ORDER BY total_likes DESC
          '''
      df_result6 = pd.read_sql_query(query6, connection)
      st.subheader("What is the total number of likes for each video, and what are their corresponding video names?")
      st.dataframe(df_result6)

    elif query_options == "What is the total number of views for each channel, and what are their corresponding channel names?":
      # total view for each channel and channel_name
      query7 = '''
          SELECT C.channel_name, SUM(V.view_count) AS total_channel_views
          FROM Channel C
          JOIN Playlist P ON C.channel_id = P.channel_id
          JOIN Video V ON P.playlist_id = V.playlist_id
          GROUP BY C.channel_name
          '''
      df_result7 = pd.read_sql_query(query7, connection)
      st.subheader("What is the total number of views for each channel, and what are their corresponding channel names?")
      st.dataframe(df_result7.head())

    elif query_options == "What are the names of all the channels that have published videos in the year 2022?":
      # channel published video in 2022
      query8 = '''
          SELECT DISTINCT C.channel_name
          FROM Channel C
          JOIN Playlist P ON C.channel_id = P.channel_id
          JOIN Video V ON P.playlist_id = V.playlist_id
          WHERE V.published_at LIKE '2022%'
          '''
      df_result8 = pd.read_sql_query(query8, connection)
      st.subheader("What are the names of all the channels that have published videos in the year 2022?")
      st.dataframe(df_result8)

    elif query_options == "What is the average duration of all videos in each channel, and what are their corresponding channel names?":
      # avg duration of all videos in each channel & channel name
      query9 = '''
          SELECT C.channel_name,
                AVG(
                  CAST(
                    ((SUBSTR(V.video_duration,3, 2)*60)+
                    CAST(SUBSTR(V.video_duration, 6, 2) AS INTEGER))/60 AS INTEGER
                  ) -- Extract seconds
                ) AS avg_video_duration_in_min
          FROM Channel C
          JOIN Playlist P ON C.channel_id = P.channel_id
          JOIN Video V ON P.playlist_id = V.playlist_id
          GROUP BY C.channel_name
          '''

      df_result9 = pd.read_sql_query(query9, connection)
      st.subheader("What is the average duration of all videos in each channel, and what are their corresponding channel names?")
      st.dataframe(df_result9)

    elif query_options == "Which videos have the highest number of comments, and what are their corresponding channel names?":
      # video with highest no of cmt and channel
      query10_1 = '''
            SELECT V.video_title, COUNT(Com.comment_id) AS comment_count, C.channel_name
            FROM Comment Com
            JOIN Video V ON Com.video_id = V.video_id
            JOIN Playlist P ON V.playlist_id = P.playlist_id
            JOIN Channel C ON P.channel_id = C.channel_id
            GROUP BY V.video_title
            ORDER BY comment_count DESC
            '''
      df_result10_1 = pd.read_sql_query(query10_1, connection)
      st.subheader("Which videos have the highest number of comments, and what are their corresponding channel names?")
      st.dataframe(df_result10_1.head(4))


if __name__ == "__main__":
  st.set_page_config(page_title= "Youtube Data Analysis",
                     page_icon= "🧊",
                     layout="wide", 
                     initial_sidebar_state="auto", 
                     menu_items= {
                    'Get Help': 'https://www.extremelycoolapp.com/help',
                    'Report a bug': "https://www.extremelycoolapp.com/bug",
                    'About': "# This is a header. This is an *extremely* cool app!"
                    })
  
  # Add custom CSS to make the title fixed at the top
  st.markdown(
      """
      <style>
      .title {
          position: fixed;
          top: 0;
          left: 0;
          width: 100%;
          background-color: white;
          z-index: 1;
          padding: 8px;
          border-bottom: 1px solid #ddd;
      }
      .title h1 {
          color: blue;
          font-style: italic; 
          margin: 1;
          margin-top: 8px;
      }
      .content {
          margin-top: 250px; /* Adjust this margin to match the title height */
      }
      </style>
      """,
      unsafe_allow_html=True,
  )

  # Title section
  st.markdown('<div class="title"><h1>Youtube Data Analysis</h1><p>Welcome to Analyze YouTube channel data and run SQL queries.</p></div>', unsafe_allow_html=True)


  
  # Content section
  st.sidebar.title("Input")
  No_of_channels = st.sidebar.number_input("Enter no of channels: ", min_value=1, max_value=10)
    
  for i in range(No_of_channels):
    channel_titles.append(st.sidebar.text_input(f"Enter channel name {i+1}: "))

  st.sidebar.title("Options")
  selected_option = st.sidebar.radio("Select an option:", ["Channel details", "Channel Video details", "Run Analysis"])

  # Define custom CSS styles for the card-like boxes
  card_style = """
      background-color: #f4f4f4;
      border: 1px solid #ddd;
      border-radius: 10px;
      padding: 15px;
      margin: 10px;
      box-shadow: 3px 3px 5px 0px rgba(0,0,0,0.3);
  """

  title_style = """
      font-weight: bold;
      color: #0072b5;
      font-size: 16px;
      margin-bottom: 10px;
  """  

  if selected_option == "Channel details":
    if st.sidebar.button("Get Selected Details"):

      for i, channel_title in enumerate(channel_titles):
        st.header(" ")
        st.header(f"Channel Details for {channel_title}")
        channel_id = get_channel_id_by_name(channel_title)
        
        if channel_id:
          channel_details = get_channel_details(channel_id)

          # Create a matrix layout
          col1, col2, col3 = st.columns(3)
          
          # Display specific details in each column
          with col1:
            st.markdown(f'<div style="{card_style}"><p style="{title_style}">Channel ID</p><p>{channel_details["channel_id"]}</p></div>', unsafe_allow_html=True)
            st.markdown(f'<div style="{card_style}"><p style="{title_style}">Channel Name</p><p>{channel_details["channel_name"]}</p></div>', unsafe_allow_html=True)
            st.markdown(f'<div style="{card_style}"><p style="{title_style}">Playlist ID</p><p>{channel_details["playlist_id"]}</p></div>', unsafe_allow_html=True)

          with col2:
              st.markdown(f'<div style="{card_style}"><p style="{title_style}">Description</p><p>{channel_details["Description"]}</p></div>', unsafe_allow_html=True)

          with col3:
              st.markdown(f'<div style="{card_style}"><p style="{title_style}">Subscribers</p><p>{channel_details["subscribers"]}</p></div>', unsafe_allow_html=True)
              st.markdown(f'<div style="{card_style}"><p style="{title_style}">Views</p><p>{channel_details["views"]}</p></div>', unsafe_allow_html=True)
              st.markdown(f'<div style="{card_style}"><p style="{title_style}">Total Videos</p><p>{channel_details["Total_videos"]}</p></div>', unsafe_allow_html=True)


        else:
          st.write(f"Channel {channel_title} not found")
            
  elif selected_option == "Channel Video details":
    if st.sidebar.button("Get Selected Details"):
      st.header(" ")
      
      
      for i, channel_title in enumerate(channel_titles):
        channel_id = get_channel_id_by_name(channel_title)
        if channel_id:
          channel_details = get_channel_details(channel_id)
          playlist_id = channel_details['playlist_id']
          playlist_details,video_ids = get_playlist_details_and_video_id(playlist_id)
          video_details = get_video_details(video_ids)
          
          st.header(f"Channel Video Details for {channel_title}")
          video_df =pd.DataFrame(video_details)
          st.dataframe(video_df, width=None, height= None)
        else:
          st.write(f"Channel {channel_title} not found")
            
  elif selected_option == "Run Analysis":
    main()