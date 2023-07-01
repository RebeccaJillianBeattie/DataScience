from csv import reader, writer
from json import load, dump

#Add publisher to json file for book chapter publications that had chapter number 0
#Also insert isbn numbers into venues_id for these book chapter publications
with open("clean_json_relational_data.json", "w", encoding="utf-8") as cleaned:
    with open("relational_other_data.json", "r", encoding="utf-8") as original:
        jsonr = load(original)

        pubs_dict = jsonr["publishers"]
        dict_to_add = {"Not_Specified": {"id":"Not_Specified", "name":"IGI Global"}}
        pubs_dict.update(dict_to_add)

        venue_id_dict = jsonr["venues_id"]
        dict_to_add_1 = {"doi:10.4018/978-1-7998-7452-2.ch016":["isbn:9781799874522"]}
        dict_to_add_2 = {"doi:10.4018/978-1-7998-5772-3.ch005":["isbn:9781799857723"]}
        venue_id_dict.update(dict_to_add_1)
        venue_id_dict.update(dict_to_add_2) 

    dump(jsonr, cleaned, ensure_ascii=False, indent=4)


with open("clean_relational_publications.csv", "w", encoding="utf-8") as cleaned:
    with open("relational_publications.csv", "r", encoding="utf-8") as original:
        publications = reader(original)
        publications_list = list(publications)
        for row in publications_list:
        
            #clean up proceedings papers presented as books
            if row[0] == "doi:10.1007/978-3-030-61244-3_16": #row 19
                row[2] = "proceedings-paper"
                row[6] = ""
                row[8] = "proceedings"
                row[10] = "22nd International Conference on Knowledge Engineering and Knowledge Management, EKAW 2020"
            
            if row[0] == "doi:10.1007/978-3-030-61244-3_6": #row 21
                row[2] = "proceedings-paper"
                row[6] = ""
                row[8] = "proceedings"
                row[10] = "22nd International Conference on Knowledge Engineering and Knowledge Management, EKAW 2020"
            
            if row[0] == "doi:10.1007/978-3-030-54956-5_2": #row 23
                row[2] = "proceedings-paper"
                row[6] = ""
                row[8] = "proceedings"
                row[10] = "24th International Conference on Theory and Practice of Digital Libraries, TPDL 2020"

            if row[0] == "doi:10.1007/978-3-030-55814-7_15": #row 25
                row[2] = "proceedings-paper"
                row[6] = ""
                row[8] = "proceedings"
                row[10] = "24th East-European Conference on Advances in Databases and Information Systems"
            
            if row[0] == "doi:10.1007/978-3-030-62466-8_28": #row 29
                row[2] = "proceedings-paper"
                row[6] = ""
                row[8] = "proceedings"
                row[10] = "19th International Semantic Web Conference, ISWC 2020"
            
            if row[0] == "doi:10.1007/978-3-030-77385-4_37": #row 33
                row[2] = "proceedings-paper"
                row[6] = ""
                row[8] = "proceedings"
                row[10] = "19th International Semantic Web Conference, ISWC 2020"

            if row[0] == "doi:10.1007/978-3-030-84825-5_11": #row 43
                row[2] = "proceedings-paper"
                row[6] = ""
                row[8] = "proceedings"
                row[10] = "9th International Conference on Cloud Computing, Big Data & Emerging Topics, JCC-BD&ET 2021"
            
            if row[0] == "doi:10.1007/978-3-030-61244-3_7": #row 63
                row[2] = "proceedings-paper"
                row[6] = ""
                row[8] = "proceedings"
                row[10] = "22nd International Conference on Knowledge Engineering and Knowledge Management, EKAW 2020"
            
            if row[0] == "doi:10.1007/978-3-030-71903-6_32": #row 69
                row[2] = "proceedings-paper"
                row[6] = ""
                row[8] = "proceedings"
                row[10] = "15th International Conference on Metadata and Semantic Research, MTSR 2021"
            
            if row[0] == "doi:10.1007/978-3-030-91669-5_24": #row 73
                row[2] = "proceedings-paper"
                row[6] = ""
                row[8] = "proceedings"
                row[10] = "23rd International Conference on Asia-Pacific Digital Libraries, ICADL 2021"
                        
            if row[0] == "doi:10.1007/978-3-319-91473-2_1": #row 591
                row[2] = "proceedings-paper"
                row[6] = ""
                row[8] = "proceedings"
                row[10] = "17th International Conference on Information Processing and Management of Uncertainty in Knowledge-Based Systems. Theory and Foundations, IPMU 2018"
            
            if row[0] == "doi:10.1007/978-981-16-6128-0_25": #row 793
                row[2] = "proceedings-paper"
                row[6] = ""
                row[8] = "proceedings"
                row[10] = "8th International Conference on Sustainable Design and Manufacturing, SDM-2021"
            
            if row[0] == "doi:10.1007/978-3-030-78570-3_4": #row 837
                row[2] = "proceedings-paper"
                row[6] = ""
                row[8] = "proceedings"
                row[10] = "International Joint conference on Industrial Engineering and Operations Management, IJCIEOM 2021"

            if row[0] == "doi:10.1007/978-3-030-58799-4_49": #row 967
                row[2] = "proceedings-paper"
                row[6] = ""
                row[8] = "proceedings"
                row[10] = "20th International Conference on Computational Science and Its Applications, ICCSA 2020"
            
            if row[0] == "doi:10.1007/978-3-030-53036-5_30": #row 971
                row[2] = "proceedings-paper"
                row[6] = ""
                row[8] = "proceedings"
                row[10] = "17th International Conference on Distributed Computing and Artificial Intelligence, DCAI 2020"

            #clean up issue format
            #when "ahead-of-print" appears (just once, row 887)
            if "ahead-of-print" in row[4]:
                row[4] = "3"
            
            #clean up volume format
            #when "Volume" appears
            if "Volume " in row[5]:
                row[5] = row[5].replace("Volume ", "")
            #when "ahead-of-print" appears (just once, row 887)
            if "ahead-of-print" in row[5]:
                row[5] = "17"
            
            #clean up when a book has chapter number 0 (happends twice, 481 and 919)
            if row[0] == "doi:10.4018/978-1-7998-7452-2.ch016":
                #remove 0 and insert real chapter number
                row[6] = "16"
                #insert publication type as book 
                row[8] = "book" 
                #state publisher not given
                row[9] = "Not_Specified" 
                #insert publication venue
                row[7] = "Reviving Businesses With New Organizational Change Management Strategies"
            
            if row[0] == "doi:10.4018/978-1-7998-5772-3.ch005":
                #remove 0 and insert real chapter number
                row[6] = "5"
                #insert publication type as book 
                row[8] = "book" 
                #state publisher not given
                row[9] = "Not_Specified" 
                #insert publication venue
                row[7] = "Enhancing Academic Research and Higher Education With Knowledge Management Principles"
    
    pubs_cleaned = writer(cleaned)
    pubs_cleaned.writerows(publications_list)





