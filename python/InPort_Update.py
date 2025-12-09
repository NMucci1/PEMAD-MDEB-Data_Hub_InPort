#################################################
##   UPDATE MDEB DATA HUB INPORT PAGES USING   ## 
##     ARCGIS REST URL METADATA XML FILES      ##
#################################################

# IMPORT LIBRARIES
import os
import requests
import oracledb
import pandas as pd
from sqlalchemy import create_engine
import xml.etree.ElementTree as ET
import re

# ACCESS ENV VARIABLES
# Use try block so script can run locally or via GitHub
try:
    from dotenv import load_dotenv
    load_dotenv(dotenv_path = os.path.expandvars(r"%USERPROFILE%\.config\secrets\.env"))
except ImportError:
    pass
# InPort credentials
inport_username = os.getenv("INPORT_USERNAME")
inport_password = os.getenv("INPORT_PASSWORD") 
inport_session_url = os.getenv("INPORT_SESSION_URL") 
inport_xml_url = os.getenv("INPORT_XML_URL") 
# Oracle credentials
tns_name = os.getenv("TNS_NAME") 
username = os.getenv("ORACLE_USERNAME")
password = os.getenv("ORACLE_PASSWORD") 
schema = os.getenv("SCHEMA")
ftr_table = os.getenv("FTR_TABLE")

# CONNECT TO ORACLE
# Enable thick mode, using oracle instant client and tnsnames.ora
oracledb.init_oracle_client()

# Connect to oracle database using SQL alchemy engine and TNS names alias
connection_string = f"oracle+oracledb://{username}:{password}@{tns_name}"
engine = create_engine(connection_string)
connection = engine.connect()

# DATA EXTRACTION FROM THE ORACLE DATABASE
# Query table to get AGOL feature services info
print("Getting dataframe of AGOL feature service metadata from Oracle...")
features_sql_query = f"SELECT * FROM {schema}.{ftr_table}"
df_features = pd.read_sql(features_sql_query, con = connection)

# OBTAIN INPORT SESSION ID 
# Define credentials 
credentials = {
    "username": inport_username,
    "password": inport_password
}

# Initialize to None so the variable always "exists"
session_id = None

try:
    # Perform the login request
    print("Obtaining InPort session ID...")
    if inport_session_url:
        response = requests.post(url=inport_session_url, json=credentials)
        response.raise_for_status()  # Check for HTTP errors
        # Extract the session ID
        data = response.json()
        session_id = data.get('sessionId')
        print("InPort session ID obtained.")
    else:
        print("Error: inport_session_url was not defined.")
except requests.exceptions.RequestException as e:
    print(f"An error occurred during the request: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")

# XML GET - MODIFY - POST LOOP
# Loop through file IDs in df_features and get AGOL XML. Then, modify XML and send to InPort via POST request.

# First, subset rows in df_features where we are managing InPort pages

# List of surveys where we manage InPort pages
surveys_with_inport = ['Hook and Line Survey', 'Marine Mammal and Sea Turtle Survey', 
'North Atlantic Right Whale Aerial Survey', 'Passive Acoustic Monitoring Survey', 
'Seal Aerial Survey', 'Turtle Ecology Survey', 'eDNA Survey']

# Subset the df_features dataframe to only include these surveys 
df_subset = df_features[df_features['survey_name'].isin(surveys_with_inport)]

# Loop through subsetted surveys
print("Looping through surveys with InPort pages to update...")

for index, row in df_subset.iterrows():
    file_id = str(row['file_id'])  # AGOL file ID
    rest_url = str(row['rest_url'])  # AGOL REST URL
    survey_name = str(row['survey_name'])
    link = str(row['link'])  # InPort link

    try:
        print(f"Starting update for {survey_name}...")
        # 1. GET XML FROM AGOL
        
        source_url = f"https://www.arcgis.com/sharing/rest/content/items/{file_id}/info/metadata/metadata.xml"
        response = requests.get(source_url)

        if response.status_code != 200:
            print(f"{survey_name} ({file_id}): Failed to fetch XML (HTTP {response.status_code})")
            continue

        root = ET.fromstring(response.content)
    
        # 2. EDIT XML

        # Edit abstract
        for abstract_node in root.findall(".//dataIdInfo/idAbs"):
            if abstract_node.text:
                # This regex pattern ensures that ALL links are removed at the start of the abstract
                # (some feature services have 2 links at the start of the abstract/description)
                match_pattern = r'(^\s*(?:<p>)?\s*)(?:<a\s+href="[^"]*">.*?</a>\s*(?:<br\s*/?>)?\s*)+'
                # flags=re.IGNORECASE | re.DOTALL ensures it works across multiple lines
                abstract_node.text = re.sub(match_pattern, r'\1', abstract_node.text, flags=re.IGNORECASE | re.DOTALL)
        print(f" - Edited XML abstract for {survey_name} -")

        # Edit use limits
        for limit_node in root.findall(".//dataIdInfo/resConst/Consts/useLimit"):
            # Removes the links and images associated with the CC0 1.0 license
            if limit_node.text:
                limit_node.text = limit_node.text[:267] + '.'
        print(f" - Edited XML use terms for {survey_name} -")

        # Remove thumbnail 
        # The thumbnail is stored as base-64 text string, this makes the XML file very large
        # This could interfere with the InPort upload
        for child in list(root):
            if child.tag.endswith("Binary"):
                for grandchild in child.iter():
                    if grandchild.tag.endswith("Thumbnail"):
                        root.remove(child)
                        print(f" - Removed thumbnail from XML for {survey_name} -")
                        break

        # Edit/create distribution info
        # Check/create <distInfo> parent
        dist_info = root.find("distInfo")
        if dist_info is None:
            dist_info = ET.SubElement(root, "distInfo")

        # Check/create <distTranOps> child
        dist_tran_ops = dist_info.find("distTranOps")
        if dist_tran_ops is None:
            dist_tran_ops = ET.SubElement(dist_info, "distTranOps")

        # Check/create <onLineSrc> 
        online_src = dist_tran_ops.find("onLineSrc")
        if online_src is None:
            online_src = ET.SubElement(dist_tran_ops, "onLineSrc")

        # Check/create <linkage> and update text
        linkage = online_src.find("linkage")
        if linkage is None:
            linkage = ET.SubElement(online_src, "linkage")
        
        linkage.text = str(rest_url)

        # Check/create <distFormat> (sibling to distTranOps)
        dist_format = dist_info.find("distFormat")
        if dist_format is None:
            dist_format = ET.SubElement(dist_info, "distFormat")

        # Check/create <formatName>
        format_name = dist_format.find("formatName")
        if format_name is None:
            format_name = ET.SubElement(dist_format, "formatName")
        
        format_name.text = "ESRI REST Service"

        print(f" - Added/edited distribution info to XML for {survey_name} -")

        # 3. SEND XML TO INPORT

        # Convert XML to string
        modified_xml = ET.tostring(root, encoding="unicode")

        # Extract InPort item ID from link in df
        id_match = re.search(r'/item/(\d+)', link)
        if id_match:
            item_id = int(id_match.group(1))
            print(f" - Updating InPort item ID: {item_id}")
        else:
            print(f"Skipping {file_id}: InPort ID not found in link '{link}'")
            continue

        # Post updated XML to InPort
        inport_update = {
            "sessionId": session_id,
            "catId": item_id,     # InPort catalog ID (number at the end of InPort link)
            "transformId": 1005,  # ArcGIS XML to InPort transform ID
            "xml": modified_xml
        }

        update = requests.post(inport_xml_url, json=inport_update)
        print(f" - InPort Status Code: {update.status_code} - ")
        print(f" - InPort Errors and Warnings: {update.text} - ")

    except Exception as e:
        print(f"{survey_name}: Error - {e}")

print("All AGOL XML items updated on InPort.")