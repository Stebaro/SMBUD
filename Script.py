# Script v2.1 - updated 06/12/2022

# Publication(IDP*, [IDA*], Pages, Abstract, Year, Title, Publisher, Type, Venue*, [refs*])
# Venue(id*, raw, Type, volume, number, date)
# FieldOfStudy(id*, Name)
# DealsWith(IDP*, fosid*, weight)
# Author(IDA*, Name, email, bio, affiliation)

import json
from lorem_text import lorem
import random
from datetime import date
import hashlib  # for fos ids
import re


def load(ds, file):
    dataString = json.dumps(ds)
    dataString = re.sub(r"[\[\]]", "", dataString)
    dataString = re.sub(r"\}, \{", "}\n{", dataString)
    if file == publications_file:
        dataString = re.sub(r", \"authors\":", "], \"authors\": [", dataString)
        dataString = re.sub(r"\}", "]}", dataString)
        dataString = re.sub(r"\"references\":",
                            "\"references\": [", dataString)
    file.write(dataString + '\n')


# Number of documents to extract and clean
n_doc = 2500
# File of the raw dataset
import_file = 'ds_raw.json'

vect_org = ['Department of Statistics, Rajshahi University, Rajshahi, Bangladesh',
            'Shinshu University', 'Nagano Prefectural College',
            'Department of Computer Science, Faculty of Engineering, Yamanashi University, Kofu, Japan',
            'Department of Computer Science & Engineering, BUET, Dhaka, Bangladesh',
            'Lodz University of Technology', 'Department of IT, NTT University, HCM City, Vietnam',
            'Department of System Design and Informatics, Kyushu Institute of Technology, Fukuka, Japan',
            'Università degli Studi di Firenze',
            'Università degli Studi di Modena e Reggio Emilia',
            'Politecnico di Bari',
            'The University of Chicago Press, Chicago, USA',
            'Department of Computer Science and Engineering, NIT Meghalaya, India',
            'Politecnico di Milano']

vect_aut_ids = []
vect_ven_ids = []
vect_fos_ids = []

# import os
# if os.path.exists("publications.json"):
#    os.remove("publications.json")
# if os.path.exists("venues.json"):
#    os.remove("venues.json")
# if os.path.exists("fos.json"):
#    os.remove("fos.json")
# if os.path.exists("rel_dw.json"):
#    os.remove("rel_dw.json")
# if os.path.exists("authors.json"):
#    os.remove("authors.json")

publications_file = open("publications.json", "w+")
venues_file = open("venues.json", "w+")
fos_file = open("fos.json", "w+")
rel_dw_file = open("rel_dw.json", "w+")
authors_file = open("authors.json", "w+")

pub = {}
ven = {}
fos = []
rel_dw = []
aut = []

with open(import_file, encoding='utf8') as json_data:
    data = json.load(json_data)
    # Vector of the ids
    vect_id = []

    for i in range(n_doc):
        # Array containing all the ids of the papers in the dataset
        vect_id.append(data[i]['id'])

    for i in range(n_doc):  # It reads only n_doc elements
        # Publisher part (authors[] filled in author part)
        pub['id'] = data[i]['id']
        pub['pages'] = random.randint(1, 20)
        pub['abstract'] = "Abstract of " + \
                          data[i]['title'] + ": " + lorem.words(5)
        pub['title'] = data[i]['title']
        if data[i]['publisher'] == "":
            data[i]['publisher'] = "PoliPrint, Milano"
        pub['publisher'] = data[i]['publisher']
        pub['type'] = data[i]['doc_type']
        if 'id' in data[i]['venue']:
            pub['venue'] = data[i]['venue']['id']
        else:
            data[i]['venue']['id'] = random.randint(4_000_000_000, 9_999_999_999)
            pub['venue'] = data[i]['venue']['id']

        pub['references'] = []
        for j in range(random.randint(1, 6)):
            # numbers contains a casual id from the vect_id
            number = vect_id[random.randint(0, n_doc - 1)]
            # Checking that numbers is not already referenced from the actual document and that has not been extracted the id of the actual document
            while number in pub['references']:
                number = vect_id[random.randint(0, n_doc - 1)]
            pub["references"].append(number)

        # Venue part
        # check if the venue was already present in the file
        if data[i]['venue']['id'] not in vect_ven_ids:
            vect_ven_ids.append(data[i]['venue']['id'])
            ven['id'] = data[i]['venue']['id']
            ven['raw'] = data[i]['venue']['raw']
            ven['volume'] = random.randint(1, 3)
            ven['number'] = random.randint(1, 20)
            if 'type' in data[i]['venue']:
                ven['type'] = data[i]['venue']['type']
            else:
                data[i]['venue']['type'] = 'J'
                ven['type'] = data[i]['venue']['type']

            ven['date'] = date.fromordinal(random.randint(date(day=1, month=1, year=data[i]['year']).toordinal(
            ), date(day=31, month=12, year=data[i]['year']).toordinal())).isoformat()
            # ven['pages'] = random.randint(pub['pages']+5, pub['pages']+50)

        if 'fos' in data[i]:
            # Relation Deals_With + Field Of Study (FOS) part
            for f_o_s in data[i]['fos']:
                obj = {}
                obj['pub_id'] = data[i]['id']
                # metodo per avere id a partire da un seed perchè non possiamo accedere agli altri fos
                obj['fos_id'] = hashlib.md5(f_o_s['name'].encode()).hexdigest()
                obj['weight'] = f_o_s['w']
                rel_dw.append(obj)
                # check if the fos was already present in the file
                if f_o_s['name'] not in vect_fos_ids:
                    obj = {}
                    vect_fos_ids.append(f_o_s['name'])
                    obj['id'] = hashlib.md5(f_o_s['name'].encode()).hexdigest()
                    obj['name'] = f_o_s['name']
                    fos.append(obj)

        # Author part
        pub['authors'] = []
        for author in data[i]['authors']:
            # check if the author was already present in the file
            if author['id'] not in vect_aut_ids:
                obj = {}
                vect_aut_ids.append(author['id'])
                obj['id'] = author['id']
                obj['name'] = author['name']
                obj['email'] = author['name'].split()[0] + '.' + \
                               author['name'].split()[len(
                                   author['name'].split()) - 1] + '@mail.com'
                obj['bio'] = 'Bio of ' + author['name'] + ': ' + lorem.words(5)
                if 'org' in author.keys():
                    obj['affiliation'] = author['org']
                else:
                    obj['affiliation'] = vect_org[random.randint(
                        0, len(vect_org) - 1)]
                aut.append(obj)
            # add authors ids in pub array
            pub['authors'].append(author['id'])

        load(pub, publications_file)
        load(ven, venues_file)
load(fos, fos_file)
load(rel_dw, rel_dw_file)
load(aut, authors_file)

publications_file.close()
venues_file.close()
fos_file.close()
rel_dw_file.close()
authors_file.close()