# -*- coding: utf-8 -*-
"""
Created on Tue Mar 15 14:36:09 2016

@author: Michael
"""

import os
import xml.etree.cElementTree as ET
import pprint
import re
import pymongo
import codecs
import json
import collections
import bson
import numpy as np
import matplotlib.pyplot as plt



DATADIR = "C:\Users\Michael\Desktop\Data Science Udacity\PS3"
DATAFILE = "syracuse_new_york.osm"
SYRACUSE_DATA = os.path.join(DATADIR, DATAFILE)

#---------------------------------------------------------------------------
#---------------------------------------------------------------------------
#Use the iterative parsing to process the map file and
#find out not only what tags are there, but also how many, to get the
#feeling on how much of which data you can expect to have in the map.

def count_tags(filename):
    counts = collections.defaultdict(int)
    for line in ET.iterparse(filename, events=("start",)):      
        current = line[1].tag
        counts[current] += 1
    return counts

syracuse_tags = count_tags(SYRACUSE_DATA)
#pprint.pprint(syracuse_tags)
#defaultdict(<type 'int'>, {'node': 275755, 'nd': 325254, 'bounds': 1, 'member': 5425, 'tag': 212438, 'osm': 1, 'way': 35199, 'relation': 775})

#---------------------------------------------------------------------------
#---------------------------------------------------------------------------
#check the "k" value for each "<tag>" and see if they can be valid keys 
#in MongoDB, as well as see if there are any other potential problems. Get a
#a count of each of each of the four tag categories and put in a dictionary

lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')


def key_type(element, keys):
    if element.tag == "tag":
        k_value = element.attrib['k']
        if lower.search(k_value) is not None:
            keys['lower'] += 1
        elif lower_colon.search(k_value) is not None:
            keys['lower_colon'] += 1
        elif problemchars.search(k_value) is not None:
            keys["problemchars"] += 1
        else:
            keys['other'] += 1

    return keys


def process_map(filename):
    keys = {"lower": 0, "lower_colon": 0, "problemchars": 0, "other": 0}
    for _, element in ET.iterparse(filename):
        keys = key_type(element, keys)

    return keys

all_keys = process_map(SYRACUSE_DATA)

#print all_keys
#{'problemchars': 0, 'lower': 98483, 'other': 7533, 'lower_colon': 106422}

#---------------------------------------------------------------------------
#---------------------------------------------------------------------------

#Audit the OSMFILE and change the variable 'mapping' to reflect the changes needed 
#to fix the unexpected street types to the appropriate ones in the expected list


street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)
 
expected = ["Street", "Avenue", "Boulevard", "Drive", "Court", "Place", "Square", "Lane", "Road", 
            "Trail", "Parkway", "Commons","Circle","Highway","Southwest", "Northeast",
           "Southeast", "Northwest","East","West","North","South","Center","Path","Plaza","Run",
           "Terrace","Way","Turnpike"]


#This mapping isn't used in this case since most of the street types fall within
#the expected list.. and the ones that don't are acceptable... I go through the process as an exercise
#and to make my code robust for uses with other cities

mapping = { "st": "Street",
            "ave": "Avenue",
            "rd": "Road",
            "w": "West",
            "n": "North",
            "s": "South",
            "e": "East",
            "blvd": "Boulevard",
            "ct": "Court",
            "dr": "Drive",
            "cir": "Circle",
            "hwy": "Highway",
            "pkwy": "Parkway",
            "sq": "Square",
            "ln": "Lane",
            "trl": "Trail",
            "pl": "Place",
            "terrace": "Terrace",
            "path": "Path",
            "plaza": "Plaza",
            "way": "Way",
            "w": "West",
            "n": "North",
            "s": "South",
            "e": "East"}
          
         
def audit_street_type(street_types, street_name):
    m = street_type_re.search(street_name)
    if m:
        street_type = m.group()
        if street_type not in expected:
            street_types[street_type].add(street_name)


def is_street_name(elem):
    return (elem.attrib['k'] == "addr:street")


def audit(osmfile):
    osm_file = open(osmfile, "r")
    street_types = collections.defaultdict(set)
    for event, elem in ET.iterparse(osm_file, events=("start",)):
        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                if is_street_name(tag):
                    audit_street_type(street_types, tag.attrib['v'])
    osm_file.close()
    return street_types


def update_name(name, mapping):
    after = []
    # Split name string to test each part individually
    for part in name.split(" "):
        # remove any extra characters in string and make all lower-case
        part = part.strip(",_\.-").lower()
        # Check each part of the name against the keys in the correction dict        
        if part in mapping.keys():
            # If is a key in the dictionary then overwrite that part of the name with the dictionary value for it
            part = mapping[part]
        # Reassemble and capitalize first letter    
        after.append(part.capitalize())
    # Return all pieces of the name as a string joined by a space.
    return " ".join(after)

#(NO FURTHER CLEANING OF STREET TYPES NEEDED...What's left over is acceptable)

#defaultdict(<type 'set'>, {'11': set(['Route 11', 'US Route 11']), 
#'31': set(['State Highway 31']), '298': set(['State Route 298']), 
#'Rowe': set(['Basile Rowe']), 'Courts': set(['Presidential Courts'])})

#---------------------------------------------------------------------------
#---------------------------------------------------------------------------
#Audit and Clean the Postal Codes

#Initially I found no strange postcode values. To further test this I decided
#to print out all of the post codes and look at the unique values by using set([])
#I found some contained "-" and I realized I needed to add this to my list of characters
#to search for. I want to only include the first 5 digits. I noticed some zip codes were 
#long (i.e. missing a "-") so for these I want to take only the first 5 digits. I also noticed
#that at least one zipcode did not have the correct +4 digits (it had +3 digits instead)

#determine if this is this an address
def is_address(elem):
    if elem.attrib['k'][:5] == "addr:":
        return True

#determine if a tag contains a postcode
def is_postcode(elem):
    return (elem.attrib['k'] == "addr:postcode")

#find all unique postal codes
data_block=[]
osm_file = open(SYRACUSE_DATA, "r")
for event, elem in ET.iterparse(osm_file, events=("start",)):
        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                if is_postcode(tag):
                    data_block.append(tag.attrib['v'])
osm_file.close()
#print(set(data_block))

#Regular Expression to identify non-5digit postal codes
contains_nonnumber_re=re.compile(r'[-a-zA-Z=:\+/&<>;\'"\?%#$@\,\. \t\r\n]')


def audit_postcode_value(postcode):
    m = contains_nonnumber_re.search(postcode)
    if m:
        postcode_value=postcode
    if len(postcode)>5:
       postcode_value=postcode
    postcode_value=postcode
    
    return postcode_value


def audit_postcode(osmfile):
    osm_file = open(osmfile, "r")
    #postcode_values = collections.defaultdict(set)
    postcode_values=[]
    for event, elem in ET.iterparse(osm_file, events=("start",)):
        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                if is_postcode(tag):
                    #audit_postcode_value(postcode_values, tag.attrib['v'])
                    postcode_values.append(audit_postcode_value(tag.attrib['v']))
    osm_file.close()
    return postcode_values

#audit_postcode(SYRACUSE_DATA)
#defaultdict(<type 'set'>, {'-': set(['13206-2238', '13218-1185', '13210-1203', 
#'13202-1107', '13210-1053', '13224-1110', '13219-331', '13204-1243'])})

def clean_postcode(postcode):
    if len(postcode)>5:
        new_postcode=int(postcode.strip()[:4])
    else:
        new_postcode=int(postcode.strip())
    return new_postcode

#---------------------------------------------------------------------------
#---------------------------------------------------------------------------
#Clean the names of pharmacies (This was a second effort given a finding when doing
#some aggregation work in MongoDB)
      
expected_pharmacy_values=["Kinney Drugs","Rite Aid Pharmacy","Walgreens","Wegmans Pharmacy",
                          "Main Street Pharmacy","Manlius Pharmacy","CVS","Gifford & West Pharmacy",
                          "Harvey's Pharmacy","Tops Pharmacy","Price Chopper Pharmacy","Etain"]
                 
pharmacy_mapping={"Rite":"Rite Aid Pharmacy",
                  "Rite-Aid":"Rite Aid Pharmacy",
                  "Kinney":"Kinney Drugs",
                  "Kinney's":"Kinney Drugs",
                  "Price" : "Price Chopper Pharmacy",
                  "Gifford":"Gifford & West Pharmacy"}
                  
def clean_pharmacy_name(pharmacy_name,pharmacy_mapping):
   
    if pharmacy_name not in expected_pharmacy_values:
        first_word=pharmacy_name.split()[0]
        new_pharmacy_name=pharmacy_mapping[first_word]
    else:
        new_pharmacy_name=pharmacy_name   
    return new_pharmacy_name
#---------------------------------------------------------------------------
#---------------------------------------------------------------------------

#CREATED = [ "version", "changeset", "timestamp", "user", "uid"]


def shape_element(element):
    node = {}
    if element.tag == "node" or element.tag == "way":
        address_info = {}
        nd_info = []

        node["type"] = element.tag
        node["id"] = element.attrib["id"]
        if "visible" in element.attrib.keys():
            node["visible"] = element.attrib["visible"]
        if "lat" in element.attrib.keys():
            node["pos"] = [float(element.attrib['lat']), float(element.attrib['lon'])]
        node["created"] = {"version": element.attrib['version'],
                            "changeset": element.attrib['changeset'],
                            "timestamp": element.attrib['timestamp'],
                            "uid": element.attrib['uid'],
                            "user": element.attrib['user']}
        for tag in element.iter("tag"):
            p = problemchars.search(tag.attrib['k'])
            if p:
                continue
            elif is_address(tag):
                if ":" in tag.attrib['k'][5:]:
                    continue
                else:
                    
                    #update the postcode
                    if tag.attrib['k'][5:] == 'postcode':
                        tag.attrib['v'] = clean_postcode(tag.attrib['v'])
                    #update the street name    
                    if tag.attrib['k'][5:] == 'street':
                        tag.attrib['v'] = update_name(tag.attrib['v'], mapping)
                                    
                    # Add the values in the address_info dictionary
                    address_info[tag.attrib['k'][5:]] = tag.attrib['v']
            else:
                    #update pharmacy names (additional data exploration)
                    if tag.attrib['k']=='name' and (tag.attrib['v'] in expected_pharmacy_values or 
                    tag.attrib['v'].split()[0] in pharmacy_mapping):
                        tag.attrib['v']=clean_pharmacy_name(tag.attrib['v'],pharmacy_mapping)
                
                    node[tag.attrib['k']] = tag.attrib['v']
        if address_info != {}:
            node['address'] = address_info
        for tag2 in element.iter("nd"):
            nd_info.append(tag2.attrib['ref'])
        if nd_info != []:
            node['node_refs'] = nd_info
        return node
    else:
        return None


def process_map(file_in, pretty = False):
    # You do not need to change this file
    file_out = "{0}.json".format(file_in)
    data = []
    with codecs.open(file_out, "w") as fo:
        for _, element in ET.iterparse(file_in):
            el = shape_element(element)
            if el:
                data.append(el)
                if pretty:
                    fo.write(json.dumps(el, indent=2)+"\n")
                else:
                    fo.write(json.dumps(el) + "\n")
    return data


data = process_map(SYRACUSE_DATA, pretty=False)


#'id': '212911507',
#  'pos': [43.119646, -76.184023],
#  'type': 'node'},
# {'created': {'changeset': '2826434',
#              'timestamp': '2009-10-12T15:49:54Z',
#              'uid': '147510',
#              'user': 'woodpeck_fixbot',
#              'version': '2'},

#---------------------------------------------------------------------------
#---------------------------------------------------------------------------
   
from pymongo import MongoClient

# Function to return a database of the name specified.
def get_db(db_name):
    client = MongoClient("mongodb://localhost:27017")
    db = client[db_name]
    return db

## Function to return the collection we want to use in MongoDB
def get_collection(db, collection):
    collections_db = db[collection]
    return collections_db

## Function to insert json data into MongoDB
def insert_data(json_data, db_collection):
    with open(json_data, 'r') as f:
        ## json.loads() takes a string, while json.load() takes a file-like object.
        for each_line in f.readlines():
            db_collection.insert(json.loads(each_line))
    print("Done.")
    

DATAFILE_json = "syracuse_new_york.osm.json"
SYRACUSE_DATA_json = os.path.join(DATADIR, DATAFILE_json)

db = get_db('ps3')
db_syracuse = get_collection(db, 'syracuse') 
insert_data(SYRACUSE_DATA_json, db_syracuse)   

#clear the database if necessary 
#db.syracuse.drop()
#---------------------------------------------------------------------------
#---------------------------------------------------------------------------
#Data Overview

#Number of Documents
db.syracuse.find().count() #310954
#Number of Nodes
db.syracuse.find({"type":"node"}).count() #275753
#Number of Ways
db.syracuse.find({"type":"way"}).count() #35182
#Number of unique users
len(db.syracuse.distinct("created.user")) #240
#Top contributer list
db.syracuse.aggregate([{"$group": {"_id":"$created.user","count":{"$sum":1}}},
                         {"$sort":{"count":-1}},{"$limit":1}])
#{ "_id" : "zeromap", "count" : 155672 }                         
                         
#makes a list                     
db.syracuse.aggregate([{"$group": {"_id":"$created.user","count":{"$sum":1}}},
                         {"$sort":{"count":-1}}])                         

#{ "_id" : "zeromap", "count" : 155672 }
#{ "_id" : "woodpeck_fixbot", "count" : 75649 }
#{ "_id" : "DTHG", "count" : 27597 }
#{ "_id" : "yhahn", "count" : 8144 }
#{ "_id" : "RussNelson", "count" : 8073 }
#{ "_id" : "fx99", "count" : 4499 }
#{ "_id" : "bot-mode", "count" : 4428 }
#{ "_id" : "timr", "count" : 2951 }
#{ "_id" : "TIGERcnl", "count" : 2077 }
#{ "_id" : "Johnc", "count" : 2037 }
#{ "_id" : "ECRock", "count" : 1853 }
#{ "_id" : "OSMF Redaction Account", "count" : 969 }
#{ "_id" : "JessAk71", "count" : 925 }
#{ "_id" : "zephyr", "count" : 806 }
#{ "_id" : "NYSDEClands", "count" : 786 }
#{ "_id" : "FrederickRelyea", "count" : 779 }
#{ "_id" : "NE2", "count" : 754 }
#{ "_id" : "D_S_W", "count" : 728 }
#{ "_id" : "cjp", "count" : 702 }
#{ "_id" : "mjpelmear", "count" : 583 }

#total count
db.syracuse.find({"created.user":{"$exists":1}}).count() #310954

#count the number of schools 
db.syracuse.aggregate([{"$match":{"amenity":{"$exists":1},"amenity":"school"}},
                       {"$group": {"_id": "null", "count": {"$sum": 1 }}}])
#{ "_id" : "null", "count" : 191 }

#count the number of shops                        
db.syracuse.aggregate([{"$match":{"shop":{"$exists":1}}},
                       {"$group": {"_id": "null", "count": {"$sum": 1 }}}])                       

#{ "_id" : "null", "count" : 858 }                     
#---------------------------------------------------------------------------
#---------------------------------------------------------------------------
#Additional Ideas

#Top 20 ammenities
db.syracuse.aggregate([{"$match":{"amenity":{"$exists":1}}},
                       {"$group":{"_id":"$amenity", "count":{"$sum":1}}},
                       {"$sort":{"count":-1}},{"$limit":20}])
                       

#{ "_id" : "parking", "count" : 922 }
#{ "_id" : "school", "count" : 191 }
#{ "_id" : "restaurant", "count" : 148 }
#{ "_id" : "bench", "count" : 148 }
#{ "_id" : "fast_food", "count" : 147 }
#{ "_id" : "place_of_worship", "count" : 128 }
#{ "_id" : "fuel", "count" : 116 }
#{ "_id" : "bank", "count" : 63 }
#{ "_id" : "post_box", "count" : 55 }
#{ "_id" : "pharmacy", "count" : 49 }
#{ "_id" : "bicycle_parking", "count" : 48 }
#{ "_id" : "waste_basket", "count" : 37 }
#{ "_id" : "cafe", "count" : 36 }
#{ "_id" : "grave_yard", "count" : 36 }
#{ "_id" : "library", "count" : 32 }
#{ "_id" : "fire_station", "count" : 31 }
#{ "_id" : "parking_entrance", "count" : 30 }
#{ "_id" : "shelter", "count" : 29 }
#{ "_id" : "toilets", "count" : 26 }
#{ "_id" : "charging_station", "count" : 25 }


#What is the most popular pharmacy
db.syracuse.aggregate([{"$match":{"amenity":{"$exists":1},"amenity":"pharmacy"}},
                       {"$group":{"_id":{"pharmacy":"$name"},"count":{"$sum":1}}},
                       {"$project":{"_id":0,"pharmacy":"$_id.pharmacy","count":"$count"}},                       
                       {"$sort":{"count":-1}}])

#*******BEFORE I CLEANED*******
#{ "count" : 11, "pharmacy" : "Kinney Drugs" }
#{ "count" : 11, "pharmacy" : "Rite Aid" }
#{ "count" : 8, "pharmacy" : "Rite Aid Pharmacy" }
#{ "count" : 3, "pharmacy" : "Walgreens" }
#{ "count" : 2, "pharmacy" : "Wegmans Pharmacy" }
#{ "count" : 1, "pharmacy" : "Rite Aid #10733" }
#{ "count" : 1, "pharmacy" : "Main Street Pharmacy of Marcellus" }
#{ "count" : 1, "pharmacy" : "Manlius Pharmacy" }
#{ "count" : 1, "pharmacy" : "CVS" }
#{ "count" : 1, "pharmacy" : "Gifford & West Pharmacy" }
#{ "count" : 1, "pharmacy" : "Harvey's Pharmacy" }
#{ "count" : 1, "pharmacy" : "Rite-Aid" }
#{ "count" : 1, "pharmacy" : "Tops Pharmacy" }
#{ "count" : 1, "pharmacy" : "Price Chopper Pharmacy" }
#{ "count" : 1, "pharmacy" : "Kinney Drugs Pharmacy" }
#{ "count" : 1, "pharmacy" : "Kinney's" }
#{ "count" : 1, "pharmacy" : "Etain" }
#{ "count" : 1, "pharmacy" : "Rite Aid #10766" }
#{ "count" : 1, "pharmacy" : "Kinney Pharmacy" }

#*******AFTER I CLEANED*******
#{ "count" : 22, "pharmacy" : "Rite Aid Pharmacy" }
#{ "count" : 14, "pharmacy" : "Kinney Drugs" }
#{ "count" : 3, "pharmacy" : "Walgreens" }
#{ "count" : 2, "pharmacy" : "Wegmans Pharmacy" }
#{ "count" : 1, "pharmacy" : "Main Street Pharmacy of Marcellus"}
#{ "count" : 1, "pharmacy" : "Manlius Pharmacy" }
#{ "count" : 1, "pharmacy" : "CVS" }
#{ "count" : 1, "pharmacy" : "Harvey's Pharmacy" }
#{ "count" : 1, "pharmacy" : "Tops Pharmacy" }
#{ "count" : 1, "pharmacy" : "Price Chopper Pharmacy" }
#{ "count" : 1, "pharmacy" : "Etain" }
#{ "count" : 1, "pharmacy" : "Gifford & West Pharmacy" }
 

#Top 5 shop types

db.syracuse.aggregate([{"$match":{"shop":{"$exists":1}}},
                       {"$group":{"_id":{"Shop":"$shop"},"count":{"$sum":1}}},
                       {"$sort":{"count":-1}},
                        {"$limit":5}])

#{ "_id" : { "Shop" : "convenience" }, "count" : 117 }
#{ "_id" : { "Shop" : "car_repair" }, "count" : 56 }
#{ "_id" : { "Shop" : "hairdresser" }, "count" : 54 }
#{ "_id" : { "Shop" : "supermarket" }, "count" : 48 }
#{ "_id" : { "Shop" : "clothes" }, "count" : 41 }


#What time of day is "zeromap" making contributions... is this user a bot?

username="zeromap"

def grab_user_data(username,osmfile):
    osm_file = open(osmfile, "r")
    time_list=[]
    time_list_raw=[]
    for event, elem in ET.iterparse(osm_file, events=("start",)):     
        if "user" in elem.attrib.keys():
            if elem.attrib['user'] == username:
                raw_time=elem.attrib['timestamp'] 
                hour_of_day=float(raw_time[11:13])+(float(raw_time[14:16])/60)+(float(raw_time[17:19])/3600)
                time_list.append(hour_of_day)
                time_list_raw=elem.attrib['timestamp']
                
    osm_file.close()
    return time_list, time_list_raw

output,output2=grab_user_data("zeromap",SYRACUSE_DATA)

output_array=np.array(output)
output_ones=np.ones_like(output_array)

#Plot a histogram show the time of day user "zeromap" is contributing to map

bins=np.arange(0,26,2, dtype=float)

plt.hist(output_array, bins)
plt.xlim(0, 24)
plt.xticks(bins)
plt.title('Timestamp of User "zeromap" Edits')
plt.xlabel('Hour During the Day (UTC)')
plt.ylabel('Number of Edits')


#References:
#http://wiki.openstreetmap.org/wiki/OSM_XML
#https://docs.python.org/2/howto/regex.html
#https://docs.python.org/3/library/xml.etree.elementtree.html
#http://nbviewer.jupyter.org/github/FCH808/FCH808.github.io/blob/master/Data%20Wrangling%20with%20MongoDB/Project/Data%20Wrangling%20with%20MongoDB%20-%20Code.ipynb
#http://fch808.github.io/Data%20Wrangling%20with%20MongoDB%20-%20Exercises.html