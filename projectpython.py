from googleapiclient.discovery import build
import pandas as pd
import streamlit as st
import mysql.connector
import datetime
import plotly.express as px
import re
import os

st.markdown("<h1 style='font-size:30px; color:yellow;text-align:center;'>YouTube Data Harvesting and Warehousing using SQL and Streamlit</h1>", unsafe_allow_html=True)

# API KEY CONNECTION
def api_connect():
    api_id = os.getenv("AIzaSyD1AeooG35pac9Cnbbjsu28mYr25jD8zcU")
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name, api_version, developerKey=api_id)
    return youtube

youtube = api_connect()

# MYSQL CONNECTION
def connect_mysql():
    try:
        conn = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST", "localhost"),
            user=os.getenv("MYSQL_USER", "root"),
            password=os.getenv("MYSQL_PASSWORD"),
            database=os.getenv("MYSQL_DATABASE", "project")
        )
        print("Connected to MySQL database")
        return conn
    except mysql.connector.Error as e:
        print(f"Error connecting to MySQL database: {e}")
        return None


def create_tables(conn):
    try:
        cursor = conn.cursor()
        
        # Create channel_details table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS channel_details(
                channel_name VARCHAR(255),
                channel_ID VARCHAR(255) PRIMARY KEY,
                Subscribers INT,
                Total_views BIGINT,
                Total_videos INT,
                channel_Description TEXT,
                playlist_ID VARCHAR(255),
                INDEX (playlist_ID)
            )
        """)
        
        # Create PLAYLIST_DETAILS table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS PLAYLIST_DETAILS(
                playlist_ID VARCHAR(255) PRIMARY KEY,
                Title VARCHAR(255),
                channel_ID VARCHAR(255),
                channel_name VARCHAR(255),
                publishedAt DATETIME,
                video_count INT,
                INDEX (channel_ID),
                FOREIGN KEY (channel_ID) REFERENCES channel_details(channel_ID)
            )
        """)
        
        # Create video_details table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS video_details(
                channel_name VARCHAR(255),
                channel_ID VARCHAR(255),
                video_id VARCHAR(255) PRIMARY KEY,
                Title VARCHAR(255),
                Tags TEXT,
                Thumbnail VARCHAR(255),
                Description TEXT,
                published_Date DATETIME,
                Duration VARCHAR(255),
                views BIGINT,
                Likes BIGINT,
                comments BIGINT,
                Favourite_count INT,
                Definition VARCHAR(255),
                Caption_status VARCHAR(255),
                INDEX (channel_ID),
                FOREIGN KEY (channel_ID) REFERENCES channel_details(channel_ID)
            )
        """)
        
        # Create comment_details table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS comment_details(
                comment_ID VARCHAR(255) PRIMARY KEY,
                video_id VARCHAR(255),
                comment_Text TEXT,
                comment_author VARCHAR(255),
                comment_published DATETIME,
                INDEX (video_id),
                FOREIGN KEY (video_id) REFERENCES video_details(video_id)
            )
        """)
        
        conn.commit()
        print('Tables created or already exist')
    except Exception as e:
        print(f"Error creating tables: {e}")

#DATA COLLECTION FUNCTION
#CHANNEL INFORMATION

def get_channel_info(channel_id):
    try:
        request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id
        )
        response = request.execute()

        if 'items' not in response or not response['items']:
            raise ValueError('No channel information found for the given channel ID')

        item = response['items'][0]  # Assuming we get one channel

        data = {
            "channel_Name": item['snippet']['title'],
            "channel_ID": item['id'],
            "Subscribers": int(item['statistics'].get('subscriberCount', 0)),
            "Total_views": int(item['statistics'].get('viewCount', 0)),
            "Total_videos": int(item['statistics'].get('videoCount', 0)),
            "channel_Description": item['snippet'].get('description', ''),
            "playlist_ID": item['contentDetails']['relatedPlaylists'].get('uploads', '')
        }

        return data

    except Exception as e:
        print(f'An error occurred: {e}')
        return None
    
#GET VIDEO IDS

def get_video_ids(channel_id):
    video_ids = []
    try:
        # Get the uploads playlist ID for the given channel
        response = youtube.channels().list(
            id=channel_id,
            part='contentDetails'
        ).execute()

        playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

        next_page_token = None
        while True:
            # Fetch videos from the uploads playlist
            response1 = youtube.playlistItems().list(
                part='snippet',
                playlistId=playlist_id,
                maxResults=50,  # You can adjust this if needed
                pageToken=next_page_token
            ).execute()

            # Collect video IDs from the response
            for item in response1.get('items', []):
                video_ids.append(item['snippet']['resourceId']['videoId'])

            # Get the next page token
            next_page_token = response1.get('nextPageToken')

            # Break the loop if no more pages
            if next_page_token is None:
                break

        return video_ids  # Return video IDs after collecting them

    except Exception as e:
        print(f"An error occurred while fetching video IDs: {e}")
        return []  # Return an empty list in case of failure

#FUNCTION TO GET VIDEO INFORMATION


def get_video_info(video_ids):
    video_data = []
    
    for video_id in video_ids:
        try:
            request = youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=video_id
            )
            response = request.execute()

            for item in response['items']:
                data = {
                    "channel_name": item['snippet'].get('channelTitle', ''),
                    "channel_ID": item['snippet'].get('channelId', ''),
                    "video_ID": item['id'],
                    "Title": item['snippet'].get('title', ''),
                    "Tags": item['snippet'].get('tags', []),
                    "Thumbnail": item['snippet']['thumbnails'].get('high', {}).get('url', ''),
                    "Description": item['snippet'].get('description', ''),
                    "published_Date": item['snippet'].get('publishedAt', ''),
                    "Duration": item['contentDetails'].get('duration', ''),
                    "Views": int(item['statistics'].get('viewCount', 0)),
                    "comments": int(item['statistics'].get('commentCount', 0)),
                    "Likes": int(item['statistics'].get('likeCount', 0)),
                    "Favourite_count": int(item['statistics'].get('favoriteCount', 0)),
                    "Definition": item['contentDetails'].get('definition', ''),
                    "Caption_status": item['contentDetails'].get('caption', '')
                }
                video_data.append(data)

        except Exception as e:
            print(f'An error occurred with video ID {video_id}: {e}')
    
    return video_data

#FUNCTION TO GET PLAYLIST DETAILS

def get_playlist_details(channel_id):
    playlist_data = []
    next_page_token = None
    
    while True:
        try:
            request = youtube.playlists().list(
                part='snippet,contentDetails',
                channelId=channel_id,
                maxResults=50,
                pageToken=next_page_token
            )
            response = request.execute()

            for playlist in response['items']:
                data = {
                    'playlist_ID': playlist['id'],
                    'playlist_Title': playlist['snippet']['title'],
                    'channel_ID': playlist['snippet']['channelId'],
                    'channel_name': playlist['snippet']['channelTitle'],
                    'publish_Date': playlist['snippet']['publishedAt'],
                    'video_count': playlist['contentDetails']['itemCount']
                }
                playlist_data.append(data)

            next_page_token = response.get('nextPageToken')
            if next_page_token is None:
                break
        
        except Exception as e:
            print(f'An error occurred: {e}')
            break
    
    return playlist_data


def get_comment_info(video_ids):
    comment_data = []
    
    for video_id in video_ids:
        next_page_token = None
        while True:
            try:
                request = youtube.commentThreads().list(
                    part='snippet',
                    videoId=video_id,
                    maxResults=100,
                    pageToken=next_page_token
                )
                response = request.execute()

                if 'items' not in response:
                    print(f'No comments found or comments are disabled for video with ID: {video_id}')
                    break

                for item in response['items']:
                    data = {
                        "comment_ID": item['snippet']['topLevelComment']['id'],
                        "video_ID": item['snippet']['topLevelComment']['snippet']['videoId'],
                        "comment_Text": item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        "comment_Author": item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        "comment_published": item['snippet']['topLevelComment']['snippet']['publishedAt']
                    }
                    comment_data.append(data)

                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break
            
            except Exception as e:
                print(f'An error occurred while fetching comments for video ID {video_id}: {e}')
                break  # Exit the loop for this video on error

    return comment_data

#FUNCTION TO UPLOAD ALL DETAILS TO MYSQL

def channel_details(channel_id):
    conn = connect_mysql()
    if conn:
        try:
            create_tables(conn)
            
            # Fetch data
            ch_details = get_channel_info(channel_id)
            pl_details = get_playlist_details(channel_id)
            vi_ids = get_videos_ids(channel_id)
            vi_details = get_video_info(vi_ids)
            com_details = get_comment_info(vi_ids)
            
            # Insert data into database if available
            if ch_details:
                insert_data(conn, 'channel_details', ch_details)
            else:
                print(f"No channel details found for channel ID: {channel_id}")
            
            if pl_details:
                insert_data(conn, 'playlist_details', pl_details)
            else:
                print(f"No playlist details found for channel ID: {channel_id}")
            
            if vi_details:
                insert_data(conn, 'video_details', vi_details)
            else:
                print(f"No video details found for channel ID: {channel_id}")
            
            if com_details:
                insert_data(conn, 'comment_details', com_details)
            else:
                print(f"No comment details found for channel ID: {channel_id}")
                
            return "Upload completed successfully"
        
        except Exception as e:
            print(f"An error occurred: {e}")
            return "An error occurred during processing"
        
        finally:
            conn.close()
    else:
        return "Error connecting to MySQL database"


def tables(channel_name):
    # Check if data exists for the channel
    news = channels_table(channel_name)

    if news:
        # If data exists, display it
        st.write(news)
    else:
        # If no data exists, create the required tables
        try:
            playlist_table(channel_name)
            videos_table(channel_name)
            comments_table(channel_name)
            st.success("Tables created successfully!")
        except Exception as e:
            st.error(f"Error creating tables: {e}")
    
    return "Operation completed"
#DISPLAY TABLES

def show_comments_table(conn):
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM comment_details")
        com_list = cursor.fetchall()
        if com_list:
            df3 = pd.DataFrame(com_list)
            st.dataframe(df3)
            return df3
        else:
            st.write("No data found in comment_details table")
            return None
    except Exception as e:
        st.write(f'Error fetching data from comment_details table: {e}')
        return None

def show_playlist_table(conn):
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM playlist_details")
        pl_list = cursor.fetchall()
        if pl_list:
            df1 = pd.DataFrame(pl_list)
            st.dataframe(df1)
            return df1
        else:
            st.write("No data found in playlist_details table")
            return None
    except Exception as e:
        st.write(f'Error fetching data from playlist_details table: {e}')
        return None

def show_videos_table(conn):
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM video_details")  # Fixed table name
        vi_list = cursor.fetchall()
        if vi_list:
            df2 = pd.DataFrame(vi_list)
            st.dataframe(df2)
            return df2
        else:
            st.write("No data found in video_details table")
            return None
    except Exception as e:
        st.write(f'Error fetching data from video_details table: {e}')
        return None

def show_channel_table(conn):
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM channel_details")  # Fixed table name
        ch_list = cursor.fetchall()
        if ch_list:
            df = pd.DataFrame(ch_list)
            st.dataframe(df)
            return df
        else:
            st.write("No data found in channel_details table")
            return None
    except Exception as e:
        st.write(f'Error fetching data from channel_details table: {e}')
        return None
        

#FUNCTION TO CHECK  IF THE CHANNEL ID ALREADY EXISTS IN MYSQL


def check_channel_exists(conn, channel_id):
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT channel_id FROM channel_details WHERE channel_id=%s", (channel_id,))
        result = cursor.fetchone()
        if result:
            return True
        else:
            return False
    except Exception as e:
        print(f'Error checking existence: {e}')
        return False


def insert_channel_details(conn, channel_id):
    try:
        cursor = conn.cursor()
        cursor.execute("INSERT INTO channel_details (channel_Id) VALUES (%s)", (channel_id,))
        conn.commit()
        return "Channel details inserted successfully"
    except Exception as e:
        print(f'Error inserting channel details: {e}')
        return 'Failed to insert channel details'


def about_the_developer():
    st.header("About the Developer")
    st.image(r"C:\Users\ELCOT\Desktop\project.jpg", caption="Yogadharshini G", width=150)
    st.subheader("Contact Details")
    st.write("Email: yogagopal2004@gmail.com")
    st.write("Phone: 7871590140")
    st.write("[LinkedIn ID]()")


def skills_take_away():
    st.header("Skills Take Away From This Project")
    st.caption("Python Scripting")
    st.caption("Data Collection")
    st.caption("Streamlit")
    st.caption("API Integration")
    st.caption("Data Management using MySQL")


def objective():
    st.header("Objective")
    st.write("Develop a Streamlit application for accessing and analyzing data from multiple YouTube channels")


def features():
    st.header("Features")
    st.write("Retrieve channel details, video information, playlist, and comments using the YouTube API")
    st.write("Store retrieved data in a MySQL database.")
    st.write("Provide query functionality for data analysis within the application.")


def workflow():
    st.header("Workflow")
    st.image("workflow_image.png", caption="Workflow", width=900)


def prerequisites():
    st.header("Prerequisites")
    st.write("Before using the application, ensure you have the following prerequisites set up:")
    st.write("1. Python Environment: Install Python on your system.")
    st.write("2. Google API Key: Obtain a Google API key from the Google Cloud Console for accessing the YouTube API.")
    st.write("3. Dependencies: Install required Python libraries using `requirements.txt`.")
    st.write("4. SQL Database: Set up a MySQL database and configure connection details in the code.")
    st.write("5. Streamlit: Install the Streamlit library for running the application.")


def required_python_libraries():
    st.header("Required Python Libraries")
    st.write("The following Python libraries are required for the project:")
    libraries = ["googleapiclient.discovery", "pandas", "streamlit", "mysql.connector", "datetime", "re"]
    st.write(libraries)


def approach():
    st.header("Approach")
    st.write("1. Set up a Streamlit app.")
    st.write("2. Connect to the YouTube API.")
    st.write("3. Store and clean data.")
    st.write("4. Migrate data to a SQL data warehouse.")
    st.write("5. Query the SQL data warehouse.")
    st.write("6. Display data in the Streamlit app.")


def queries():
    st.header("Queries")
    pass


def main():
    col1, col2 = st.columns(2)
    with col1:
        st.header("Navigation")
        options = ["About the Developer", "Skills Take Away From This Project", "Objective", "Features",
                   "Workflow", "Prerequisites", "Required Python Libraries", "Approach"]
        choice = st.radio("Go to", options)
    with col2:
        if choice == "About the Developer":
            about_the_developer()
        elif choice == "Skills Take Away From This Project":
            skills_take_away()
        elif choice == "Objective":
            objective()
        elif choice == "Features":
            features()
        elif choice == "Workflow":
            workflow()
        elif choice == "Prerequisites":
            prerequisites()
        elif choice == "Required Python Libraries":
            required_python_libraries()
        elif choice == "Approach":
            approach()


if __name__ == '__main__':
    main()

# Input and button for collecting channel data
channel_id = st.text_input("Enter the channel ID")

if st.button("Collect and Store Data"):
    conn = connect_mysql()
    if conn:
        if check_channel_exists(conn, channel_id):
            st.success("Channel details for the given channel ID already exist.")
        else:
            result = channel_details(channel_id)
            st.success(result)
            conn.close()
    else:
        st.error("Failed to connect to MySQL database.")

# FUNCTION TO RETRIEVE ALL CHANNEL NAMES FROM MYSQL
def get_all_channels(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT channel_name FROM channel_details")
        channels = cursor.fetchall()
        cursor.close()  # Close the cursor after execution
        return [channel[0] for channel in channels]
    except Exception as e:
        print(f'Error retrieving channels: {e}')
        return []

# FUNCTION TO DISPLAY TABLES BASED ON THE SELECTED CHANNEL
def display_tables(channel_name):
    # Implement logic if you want to fetch data based on the selected channel_name
    st.write(f"Displaying data for channel: {channel_name}")
    # You can extend this to display specific data related to this channel

# MAIN CONTENT
conn = connect_mysql()
if conn:
    all_channels = get_all_channels(conn)
    
    # Select box to choose which table to view
    show_table = st.selectbox("CHOOSE THE TABLE FOR VIEW", ("CHANNELS", "PLAYLISTS", "VIDEOS", "COMMENTS"))

    # Display the selected table
    if show_table == "CHANNELS":
        show_channel_table(conn)
    elif show_table == "PLAYLISTS":
        show_playlist_table(conn)
    elif show_table == "VIDEOS":
        show_videos_table(conn)
    elif show_table == "COMMENTS":
        show_comments_table(conn)
    
    conn.close()  # Close the connection after the table is shown

# FUNCTION TO EXECUTE A QUERY AND RETURN A DATAFRAME
def execute_query(conn, query):
    try:
        df = pd.read_sql(query, conn)  # Use Pandas to execute the SQL query
        return df
    except Exception as e:
        st.error(f'Error executing query: {e}')
        return None
queries = {
    "1. Video names and their channels?": 
    "SELECT Title AS Video_Name, Channel_Name FROM video_details",

    "2. Channels with most videos & count?": 
    "SELECT Channel_Name, COUNT(*) AS Video_Count FROM video_details GROUP BY Channel_Name ORDER BY Video_Count DESC LIMIT 5",

    "3. Top-10-viewed videos & channels?": 
    "SELECT Title AS Video_Name, Channel_Name, Views FROM video_details ORDER BY Views DESC LIMIT 10",

    "4. Comments for each video & their names?": 
    "SELECT v.Title AS Video_Name, COUNT(c.comment_ID) AS Comment_Count FROM video_details v LEFT JOIN comment_details c ON v.video_ID = c.video_ID GROUP BY v.Title",

    "5. Most liked videos & their channels?": 
    "SELECT Title AS Video_Name, Channel_Name, Likes FROM video_details ORDER BY Likes DESC LIMIT 10", 

    "6. Total likes per video & their names?": 
    "SELECT Title AS Video_Name, SUM(Likes) AS Total_Likes FROM video_details GROUP BY Title",

    "7. Total views per channel & their names?": 
    "SELECT Channel_Name, SUM(Views) AS Total_Views FROM video_details GROUP BY Channel_Name",

    "8. Channels with videos in 2022?": 
    "SELECT DISTINCT Channel_Name FROM video_details WHERE YEAR(Published_Date) = 2022",

    "9. Avg duration per channel & their names?": 
    "SELECT Channel_Name, AVG(TIME_TO_SEC(TIMEDIFF(STR_TO_DATE(Duration, '%H:%i:%s'), STR_TO_DATE('00:00:00', '%H:%i:%s')))) AS Average_Duration FROM video_details GROUP BY Channel_Name",

    "10. Most commented videos & their channels?": 
    "SELECT v.Title AS Video_Name, v.Channel_Name AS Channel_Name, COUNT(c.comment_ID) AS Comment_Count FROM video_details v LEFT JOIN comment_details c ON v.video_ID = c.video_ID GROUP BY v.Title, v.Channel_Name ORDER BY Comment_Count DESC LIMIT 10"
}

st.markdown(
    "<h1 style='font-size:20px; color:yellow; text-align:center;'>YouTube Data Harvesting - ML YouTube Data Harvesting and Warehousing using SQL and Streamlit</h1>", 
    unsafe_allow_html=True
)
#STREAMLIT APP

if st.button("Run query"):
    if conn:
        df = execute_query(conn, queries[Selected_query])
        if df is not None:
            st.write(df)

            # Visualization logic based on the result columns
            if "Video_Name" in df.columns and "Views" in df.columns:
                fig = px.bar(df, x="Video_Name", y="Views", title="Top 10 viewed videos", labels={"Views": 'Number of views'})
                st.plotly_chart(fig)
            
            elif "Channel_Name" in df.columns and "Video_Count" in df.columns:
                fig = px.bar(df, x="Channel_Name", y="Video_Count", title="Channels with most videos", labels={"Video_Count": 'Number of videos'})
                st.plotly_chart(fig)

            elif "Video_Name" in df.columns and "Likes" in df.columns:
                fig = px.bar(df, x="Video_Name", y="Likes", title="Most liked videos", labels={"Likes": 'Number of Likes'})
                st.plotly_chart(fig)

            elif "Channel_Name" in df.columns and "Total_Views" in df.columns:
                fig = px.bar(df, x="Channel_Name", y="Total_Views", title="Total views per channel", labels={"Total_Views": 'Number of views'})
                st.plotly_chart(fig)

            elif "Channel_Name" in df.columns and "Average_Duration" in df.columns:
                df["Average_Duration"] = df["Average_Duration"] / 60  # Converting seconds to minutes
                fig = px.bar(df, x="Channel_Name", y="Average_Duration", title="Average duration per channel", labels={"Average_Duration": 'Duration (minutes)'})
                st.plotly_chart(fig)
        else:
            st.write("No result found.")
        conn.close()
    else:
        st.write("Failed to connect to MySQL database.")
